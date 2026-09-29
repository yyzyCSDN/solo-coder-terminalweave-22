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
    append_fsync,
    file_lock,
    read_verified_lines,
    stream_filename,
    truncate_at,
)


@dataclass(frozen=True)
class DurableStoredEvent:
    stream_id: str
    sequence: int
    event_type: str
    payload: dict[str, object]
    occurred_at: datetime
    command_id: str | None = None
    checksum: str = ''


def _record_checksum(stream_id: str, sequence: int, event_type: str,
                     payload: dict[str, object], occurred_at: str,
                     command_id: str | None) -> str:
    body = {
        'stream_id': stream_id,
        'sequence': sequence,
        'event_type': event_type,
        'payload': payload,
        'occurred_at': occurred_at,
        'command_id': command_id,
    }
    return PayloadChecksum.digest(body)


class DurableEventStore:
    """Append-only per-stream event logs on disk.

    Every record carries a SHA-256 checksum, is flushed and fsynced before an
    append is acknowledged, and is ordered by a per-stream 1-based sequence.
    Appends use optimistic concurrency: callers must pass the sequence they
    expect the stream to currently be at. A single per-directory ``fcntl``
    lock makes that check-and-append atomic across processes.
    """

    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._events_dir = self.directory / 'events'
        self._events_dir.mkdir(parents=True, exist_ok=True)
        self._lock_path = self.directory / '.events.lock'
        self._lock_path.touch(exist_ok=True)
        self._mem_lock = RLock()

    def _path(self, stream_id: str) -> Path:
        return self._events_dir / (stream_filename(stream_id) + '.jsonl')

    def append(self, stream_id: str, expected_sequence: int, event_type: str,
               payload: dict[str, object], occurred_at: datetime | None = None,
               command_id: str | None = None) -> DurableStoredEvent:
        if not isinstance(event_type, str) or not event_type.strip():
            raise ValidationError('event type required')
        if not isinstance(payload, dict):
            raise ValidationError('payload must be a mapping')
        if command_id is not None and not isinstance(command_id, str):
            raise ValidationError('command_id must be a string')
        when = ensure_utc(occurred_at or utc_now())
        when_iso = iso_z(when)
        with self._mem_lock, open(self._lock_path, 'a+') as flock, file_lock(
                flock.fileno()):
            events, _repaired = self._read_locked(stream_id)
            current = events[-1].sequence if events else 0
            if current != expected_sequence:
                raise ConflictError(
                    f'stream {stream_id} expected {expected_sequence}, '
                    f'actual {current}')
            if command_id is not None and any(
                    e.command_id == command_id for e in events):
                # a client retried a command after the event was durably
                # stored; the optimistic check alone would accept it
                raise ConflictError(
                    f'stream {stream_id}: command_id {command_id} already applied')
            sequence = current + 1
            checksum = _record_checksum(stream_id, sequence, event_type,
                                        payload, when_iso, command_id)
            record = {
                'stream_id': stream_id,
                'sequence': sequence,
                'event_type': event_type,
                'payload': payload,
                'occurred_at': when_iso,
                'command_id': command_id,
                'checksum': checksum,
            }
            line = json.dumps(record, sort_keys=True, separators=(',', ':'),
                              ensure_ascii=False, default=str)
            append_fsync(self._path(stream_id), line)
            return DurableStoredEvent(stream_id, sequence, event_type,
                                      dict(payload), when, command_id,
                                      checksum)

    def read(self, stream_id: str,
             after_sequence: int = 0,
             repair_tail: bool = True) -> tuple[DurableStoredEvent, ...]:
        if after_sequence < 0:
            raise ValidationError('after_sequence cannot be negative')
        events, _ = self.load(stream_id, repair_tail=repair_tail)
        return tuple(e for e in events if e.sequence > after_sequence)

    def current_sequence(self, stream_id: str) -> int:
        events, _ = self.load(stream_id)
        return events[-1].sequence if events else 0

    def all_streams(self) -> tuple[str, ...]:
        return tuple(sorted(
            p.stem for p in self._events_dir.glob('*.jsonl') if p.is_file()))

    def load(self, stream_id: str, repair_tail: bool = True
             ) -> tuple[list[DurableStoredEvent], bool]:
        """Return ``(verified_events, torn_tail_repaired)`` for a stream."""
        return self._load(stream_id, repair_tail=repair_tail)

    def _load(self, stream_id: str, repair_tail: bool = True
              ) -> tuple[list[DurableStoredEvent], bool]:
        """Load and verify a stream log; repair a torn tail line in place.

        Readers take a shared flock; appenders hold an exclusive one, so a
        reader never observes a half-written record. Truncating a torn tail
        (a crash from a *previous* process) upgrades to an exclusive lock and
        re-reads under it.
        """
        path = self._path(stream_id)
        with self._mem_lock, open(self._lock_path, 'a+') as flock:
            with file_lock(flock.fileno(), shared=True):
                entries, good_bytes = read_verified_lines(path)
                torn = path.exists() and good_bytes < path.stat().st_size
            if torn and repair_tail:
                # exclusive recheck+repair (shared lock released on exit)
                with file_lock(flock.fileno()):
                    entries, good_bytes = read_verified_lines(path)
                    torn = path.exists() and good_bytes < path.stat().st_size
                    if torn:
                        truncate_at(path, good_bytes)
        events = self._decode_entries(stream_id, entries)
        return events, bool(torn and repair_tail)

    def _read_locked(self, stream_id: str) -> tuple[list[DurableStoredEvent], bool]:
        """Read while the caller already holds the exclusive file lock."""
        path = self._path(stream_id)
        entries, good_bytes = read_verified_lines(path)
        torn = path.exists() and good_bytes < path.stat().st_size
        if torn:
            truncate_at(path, good_bytes)
        return self._decode_entries(stream_id, entries), torn

    @staticmethod
    def _decode_entries(stream_id: str, entries) -> list[DurableStoredEvent]:
        events: list[DurableStoredEvent] = []
        expected = 0
        for data, complete, valid_json in entries:
            if not valid_json:
                if not complete:
                    continue
                raise ValidationError(
                    f'stream {stream_id}: malformed event record')
            if not complete:
                # parseable but unfinished tail: only safe to trust after
                # checksum verification succeeds
                try:
                    DurableEventStore._verify_record(data, stream_id)
                except ValidationError:
                    continue
            else:
                DurableEventStore._verify_record(data, stream_id)
            event = DurableStoredEvent(
                str(data['stream_id']),
                int(data['sequence']),
                str(data['event_type']),
                dict(data['payload']),
                parse_iso_z(str(data['occurred_at'])),
                data.get('command_id'),
                str(data.get('checksum', '')),
            )
            expected += 1
            if event.sequence != expected:
                raise ValidationError(
                    f'stream {stream_id}: sequence gap at {event.sequence}, '
                    f'expected {expected}')
            events.append(event)
        return events

    @staticmethod
    def _verify_record(data: dict, stream_id: str) -> None:
        required = {'stream_id', 'sequence', 'event_type', 'payload',
                    'occurred_at', 'checksum'}
        missing = required - data.keys()
        if missing:
            raise ValidationError(
                f'stream {stream_id}: event record missing fields {sorted(missing)}')
        if not isinstance(data['payload'], dict):
            raise ValidationError(f'stream {stream_id}: payload is not a mapping')
        checksum = str(data['checksum'])
        actual = PayloadChecksum.digest({
            'stream_id': data['stream_id'],
            'sequence': data['sequence'],
            'event_type': data['event_type'],
            'payload': data['payload'],
            'occurred_at': data['occurred_at'],
            'command_id': data.get('command_id'),
        })
        if actual != checksum.lower():
            raise ValidationError(
                f'stream {stream_id}: checksum mismatch at sequence '
                f'{data.get("sequence")}')
