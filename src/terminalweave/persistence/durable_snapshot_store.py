from __future__ import annotations
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from terminalweave.core.checksum import PayloadChecksum
from terminalweave.core.clock import ensure_utc, iso_z, parse_iso_z, utc_now
from terminalweave.core.errors import ConflictError, ValidationError
from terminalweave.persistence._durability import (
    _fsync_dir,
    atomic_write,
    stream_filename,
)


@dataclass(frozen=True)
class DurableSnapshot:
    stream_id: str
    sequence: int
    state: dict[str, object]
    created_at: datetime
    checksum: str = ''


class DurableSnapshotStore:
    """One JSON snapshot file per stream, written atomically and checksummed.

    Snapshots are a fast path for recovery, never the source of truth: if a
    snapshot file is missing, truncated, fails its checksum, or claims a
    sequence newer than the event log, it is treated as unusable and recovery
    falls back to replaying events. A corrupt file is quarantined (renamed
    with a ``.corrupt`` suffix) rather than deleted.
    """

    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self._snap_dir = self.directory / 'snapshots'
        self._snap_dir.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()

    def _path(self, stream_id: str) -> Path:
        return self._snap_dir / (stream_filename(stream_id) + '.json')

    def put(self, stream_id: str, sequence: int, state: dict[str, object],
            created_at: datetime | None = None,
            max_sequence: int | None = None) -> DurableSnapshot:
        if sequence < 0:
            raise ValidationError('snapshot sequence cannot be negative')
        if not isinstance(state, dict):
            raise ValidationError('snapshot state must be a mapping')
        if max_sequence is not None and sequence > max_sequence:
            raise ConflictError(
                f'refusing snapshot at sequence {sequence} ahead of event log '
                f'{max_sequence}')
        when = ensure_utc(created_at or utc_now())
        when_iso = iso_z(when)
        with self._lock:
            current = self.get(stream_id)
            if current is not None and sequence < current.sequence:
                raise ConflictError('cannot replace snapshot with older sequence')
            body = {
                'stream_id': stream_id,
                'sequence': sequence,
                'state': state,
                'created_at': when_iso,
            }
            checksum = PayloadChecksum.digest(body)
            record = dict(body)
            record['checksum'] = checksum
            atomic_write(self._path(stream_id),
                         json.dumps(record, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False, default=str))
            return DurableSnapshot(stream_id, sequence, state, when, checksum)

    def delete(self, stream_id: str) -> bool:
        """Remove a snapshot (e.g. one proven ahead of the event log)."""
        path = self._path(stream_id)
        with self._lock:
            if not path.exists():
                return False
            path.unlink()
            _fsync_dir(path.parent)
            return True

    def get(self, stream_id: str) -> DurableSnapshot | None:
        path = self._path(stream_id)
        with self._lock:
            if not path.exists():
                return None
            try:
                data = json.loads(path.read_text(encoding='utf-8'))
                body = {k: data[k] for k in
                        ('stream_id', 'sequence', 'state', 'created_at')}
                if data['stream_id'] != stream_id:
                    raise ValidationError('snapshot stream id mismatch')
                if PayloadChecksum.digest(body) != str(data['checksum']).lower():
                    raise ValidationError('snapshot checksum mismatch')
                if not isinstance(data['sequence'], int) or data['sequence'] < 0:
                    raise ValidationError('snapshot sequence invalid')
                if not isinstance(data['state'], dict):
                    raise ValidationError('snapshot state is not a mapping')
                return DurableSnapshot(
                    stream_id,
                    int(data['sequence']),
                    dict(data['state']),
                    parse_iso_z(str(data['created_at'])),
                    str(data['checksum']),
                )
            except (ValueError, KeyError, TypeError, ValidationError):
                self._quarantine(path)
                return None

    @staticmethod
    def _quarantine(path: Path) -> None:
        stamp = utc_now().strftime('%Y%m%d%H%M%S%f')
        target = path.with_suffix(path.suffix + f'.corrupt-{stamp}')
        try:
            os.replace(path, target)
            _fsync_dir(path.parent)
        except OSError:
            try:
                path.unlink()
            except OSError:
                pass
