from __future__ import annotations
import json
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from terminalweave.core.errors import ValidationError

try:  # POSIX advisory locks; ports elsewhere fall back to a no-op
    import fcntl
    _LOCK_EX = fcntl.LOCK_EX
    _LOCK_SH = fcntl.LOCK_SH
    _LOCK_UN = fcntl.LOCK_UN
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None
    _LOCK_EX = _LOCK_SH = _LOCK_UN = 0


@contextmanager
def file_lock(fileno: int, shared: bool = False):
    if fcntl is not None:
        fcntl.flock(fileno, _LOCK_SH if shared else _LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fileno, _LOCK_UN)
    else:  # pragma: no cover
        yield


_STREAM_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$')


def stream_filename(stream_id: str) -> str:
    """Map a stream id to its file name, rejecting path traversal input."""
    if not isinstance(stream_id, str) or not _STREAM_ID.fullmatch(stream_id):
        raise ValidationError(f'invalid stream id: {stream_id!r}')
    return stream_id


def atomic_write(path: Path, text: str) -> None:
    """Write ``text`` durably and atomically (tmp file + fsync + os.replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=str(path.parent))
    tmp_path = Path(tmp)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)


def append_fsync(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        f.write(line + '\n')
        f.flush()
        os.fsync(f.fileno())


def _fsync_dir(directory: Path) -> None:
    try:
        fd = os.open(str(directory), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def read_verified_lines(path: Path):
    """Read a durable JSONL log, returning ``(entries, good_bytes)``.

    Each entry is ``(record_or_none, complete, valid_json)``. A final line
    without a trailing newline is an incomplete tail (crash mid-append) and
    is excluded from ``good_bytes``; the caller truncates to ``good_bytes``
    under an exclusive log lock. A non-JSON *complete* line is hard
    corruption (record_or_none is None, valid_json False) and the caller
    must raise rather than truncate.
    """
    if not path.exists():
        return [], 0
    raw = path.read_bytes()
    entries = []
    good_bytes = 0
    chunks = raw.splitlines(keepends=True)
    for chunk in chunks:
        complete = chunk.endswith(b'\n')
        text = chunk.decode('utf-8').strip()
        if not text:
            if complete:
                good_bytes += len(chunk)
            continue
        try:
            data = json.loads(text)
            if not isinstance(data, dict):
                raise ValueError('record is not an object')
        except (ValueError, UnicodeDecodeError):
            entries.append((None, complete, False))
            if complete:
                good_bytes += len(chunk)
            continue
        entries.append((data, complete, True))
        if complete:
            good_bytes += len(chunk)
    return entries, good_bytes


def truncate_at(path: Path, length: int) -> bool:
    if length >= path.stat().st_size:
        return False
    with path.open('r+b') as f:
        f.truncate(length)
        f.flush()
        os.fsync(f.fileno())
    return True
