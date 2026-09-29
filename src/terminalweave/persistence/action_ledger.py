from __future__ import annotations
import json
import os
from pathlib import Path
from threading import RLock
from typing import Callable
from terminalweave.core.errors import ValidationError
from terminalweave.persistence._durability import (
    append_fsync,
    atomic_write,
    read_verified_lines,
)

ActionHandler = Callable[[str, str, dict[str, object]], None]


def action_key(stream_id: str, sequence: int) -> str:
    return f'{stream_id}:{sequence}'


class ActionLedger:
    """Durable record of which event-driven business actions have executed.

    The ledger is an append-only JSONL of status transitions (``pending`` /
    ``completed`` / ``failed``) keyed by ``stream_id:sequence``. Periodic
    compaction rewrites one record per key (deleting keys whose action is
    known complete). On recovery this lets the store distinguish an event
    whose action never ran from one whose action already committed: completed
    actions are never dispatched a second time, even across many restarts.
    """

    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / 'action-ledger.jsonl'
        self._lock = RLock()
        self._status: dict[str, str] = {}
        self._dirty = 0
        self._reload()

    def _reload(self) -> None:
        entries, _ = read_verified_lines(self.path)
        for data, complete, valid_json in entries:
            if not complete:
                continue
            if not valid_json:
                raise ValidationError('corrupt action ledger record')
            key = data.get('key')
            status = data.get('status')
            if not isinstance(key, str) or status not in ('pending', 'completed', 'failed'):
                raise ValidationError(f'invalid action ledger record: {data}')
            self._status[key] = status

    def status_of(self, key: str) -> str | None:
        with self._lock:
            return self._status.get(key)

    def mark_pending(self, key: str) -> None:
        self._write(key, 'pending')

    def mark_completed(self, key: str) -> None:
        self._write(key, 'completed')

    def mark_failed(self, key: str, error: str) -> None:
        self._write(key, 'failed', {'error': error[:500]})

    def _write(self, key: str, status: str, extra: dict | None = None) -> None:
        with self._lock:
            record = {'key': key, 'status': status}
            if extra:
                record.update(extra)
            append_fsync(self.path,
                         json.dumps(record, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False, default=str))
            self._status[key] = status
            self._dirty += 1

    def pending_keys(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(k for k, v in self._status.items()
                                if v in ('pending', 'failed')))

    def compact(self, drop_completed_up_to: dict[str, int] | None = None) -> int:
        """Rewrite the log, dropping only provably-finished records.

        A ``completed`` record for ``plan:sequence`` is dropped only when that
        plan has a durable snapshot at or past the sequence
        (``drop_completed_up_to={plan: sequence}``): even if the event log is
        later replayed from scratch (snapshot lost/corrupt) the action never
        runs again *for sequences past that point*, and inside it the event is
        folded into state without redispatch. Pending/failed records and all
        records without a snapshot boundary are always retained. Returns the
        number of records removed.
        """
        drop_completed_up_to = drop_completed_up_to or {}
        with self._lock:
            rows = []
            for data, complete, valid_json in read_verified_lines(self.path)[0]:
                if not complete:
                    continue
                if not valid_json:
                    raise ValidationError('corrupt action ledger record')
                rows.append(data)
            latest: dict[str, dict[str, object]] = {}
            for data in rows:
                latest[str(data['key'])] = data
            before = len(rows)
            for key in list(latest):
                record = latest[key]
                if record.get('status') != 'completed':
                    continue
                pid, _, seq_text = key.rpartition(':')
                boundary = drop_completed_up_to.get(pid)
                if boundary is not None and seq_text.isdigit() \
                        and int(seq_text) <= boundary:
                    del latest[key]
            if latest:
                text = ''.join(
                    json.dumps(latest[k], sort_keys=True, separators=(',', ':'),
                               ensure_ascii=False, default=str) + '\n'
                    for k in sorted(latest))
                atomic_write(self.path, text)
            elif self.path.exists():
                atomic_write(self.path, '')
            self._status = {k: str(r['status']) for k, r in latest.items()}
            self._dirty = len(latest)
            return before - self._dirty
