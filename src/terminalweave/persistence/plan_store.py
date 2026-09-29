from __future__ import annotations
import json
import os
import re
import tempfile
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Callable
from terminalweave.core.checksum import PayloadChecksum
from terminalweave.core.clock import ensure_utc, iso_z, parse_iso_z, utc_now
from terminalweave.core.errors import ConflictError, ValidationError
from terminalweave.core.serialization import to_primitive

try:
    import fcntl
except ImportError:  # pragma: no cover - non-posix platforms
    fcntl = None

PlanReducer = Callable[[dict[str, object], 'PlanEvent'], dict[str, object]]
PlanEffectHandler = Callable[['PlanEvent', dict[str, object]], None]

_PLAN_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$')
_EVENTS_FILE = 'events.jsonl'
_SNAPSHOT_FILE = 'snapshot.json'
_PREV_SNAPSHOT_FILE = 'snapshot.prev.json'
_LOCK_FILE = '.lock'
_SNAPSHOT_TMP_PREFIX = '.snapshot.'
_EVENT_TMP_PREFIX = '.events.'
_JSON_KW = {'sort_keys': True, 'separators': (',', ':'), 'ensure_ascii': False}


@dataclass(frozen=True)
class PlanEvent:
    plan_id: str
    sequence: int
    event_type: str
    payload: dict[str, object]
    action_id: str
    occurred_at: datetime

    def __post_init__(self):
        object.__setattr__(self, 'occurred_at', ensure_utc(self.occurred_at))
        _validate_identifier(self.plan_id, 'plan_id')
        if self.sequence < 1:
            raise ValidationError('event sequence must be positive')
        if not self.event_type.strip():
            raise ValidationError('event type required')
        if not self.action_id.strip():
            raise ValidationError('action id required')


@dataclass(frozen=True)
class PlanState:
    version: int
    data: dict[str, object]
    applied_actions: dict[str, int]

    def copied(self) -> 'PlanState':
        return PlanState(self.version, deepcopy(self.data), dict(self.applied_actions))


@dataclass(frozen=True)
class PlanSnapshot:
    plan_id: str
    sequence: int
    data: dict[str, object]
    applied_actions: dict[str, int]
    created_at: datetime | None = None


@dataclass(frozen=True)
class CommandOutcome:
    plan_id: str
    sequence: int
    version: int
    duplicate: bool
    state: PlanState


@dataclass(frozen=True)
class PlanRecovery:
    plan_id: str
    version: int
    snapshot_sequence: int
    events_replayed: int
    snapshots_discarded: int
    events_truncated: int
    snapshot_ahead: bool


@dataclass(frozen=True)
class RecoveryReport:
    plans: tuple[PlanRecovery, ...]

    @property
    def plans_recovered(self) -> int:
        return len(self.plans)

    @property
    def events_replayed(self) -> int:
        return sum(p.events_replayed for p in self.plans)

    @property
    def snapshots_discarded(self) -> int:
        return sum(p.snapshots_discarded for p in self.plans)

    @property
    def events_truncated(self) -> int:
        return sum(p.events_truncated for p in self.plans)


def _validate_identifier(value: object, label: str) -> None:
    if not isinstance(value, str) or not _PLAN_ID_RE.match(value):
        raise ValidationError(f'invalid {label}: {value!r}')


def _deep_merge(target: dict[str, object], source: dict[str, object]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = deepcopy(value)


def default_plan_reducer(data: dict[str, object], event: PlanEvent) -> dict[str, object]:
    merged = deepcopy(data)
    _deep_merge(merged, event.payload)
    return merged


def _event_record(event: PlanEvent) -> dict[str, object]:
    return {
        'plan_id': event.plan_id,
        'sequence': event.sequence,
        'event_type': event.event_type,
        'payload': event.payload,
        'action_id': event.action_id,
        'occurred_at': iso_z(event.occurred_at),
    }


def _event_digest(event: PlanEvent) -> str:
    return PayloadChecksum.digest(_event_record(event))


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class DurablePlanStore:
    """Restartable plan storage backed by ordered events and periodic snapshots.

    Each plan is its own event stream: key plan changes are appended to
    ``events.jsonl`` with gap-free sequences and per-record checksums. A
    snapshot is written atomically every ``snapshot_every`` events, keeping
    the previous generation as a fallback.

    Recovery loads the newest valid snapshot and folds every later event;
    a corrupt snapshot is discarded (falling back to the previous generation,
    then to a full event replay) and a torn event-log tail is truncated.
    Folding is pure: the optional ``effect_handler`` only runs for genuinely
    new commands, so recovering twice never re-runs completed business
    actions. Commands carry ``expected_version`` for optimistic concurrency
    and ``action_id`` for idempotent retries.
    """

    def __init__(self, directory: str | Path, *, snapshot_every: int=20,
                 reducer: PlanReducer | None=None,
                 effect_handler: PlanEffectHandler | None=None,
                 clock: Callable[[], datetime]=utc_now):
        if snapshot_every < 1:
            raise ValidationError('snapshot_every must be positive')
        self._dir = Path(directory)
        self._snapshot_every = snapshot_every
        self._reducer: PlanReducer = reducer or default_plan_reducer
        self._effect_handler = effect_handler
        self._clock = clock
        self._lock = RLock()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock_fd = os.open(self._dir / _LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o600)
        self._states: dict[str, PlanState] = {}
        self._ahead: set[str] = set()
        self.last_recovery: RecoveryReport | None = None
        self.recover()

    @classmethod
    def open(cls, directory: str | Path, **kwargs) -> 'DurablePlanStore':
        return cls(directory, **kwargs)

    def __enter__(self) -> 'DurablePlanStore':
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            if self._lock_fd is not None:
                os.close(self._lock_fd)
                self._lock_fd = None

    def _plan_dir(self, plan_id: str) -> Path:
        return self._dir / plan_id

    @contextmanager
    def _exclusive(self):
        if fcntl is not None and self._lock_fd is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        else:  # pragma: no cover - non-posix fallback
            yield

    # ---------------------------------------------------------------- writes

    def _append_event(self, event: PlanEvent) -> None:
        plan_dir = self._plan_dir(event.plan_id)
        plan_dir.mkdir(parents=True, exist_ok=True)
        record = _event_record(event)
        record['checksum'] = PayloadChecksum.digest(record)
        path = plan_dir / _EVENTS_FILE
        with path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(record, **_JSON_KW) + '\n')
            f.flush()
            os.fsync(f.fileno())

    def _rewrite_events(self, plan_id: str, events: list[PlanEvent]) -> None:
        plan_dir = self._plan_dir(plan_id)
        fd, tmp = tempfile.mkstemp(prefix=_EVENT_TMP_PREFIX, suffix='.tmp', dir=str(plan_dir))
        tmp_path = Path(tmp)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                for event in events:
                    record = _event_record(event)
                    record['checksum'] = PayloadChecksum.digest(record)
                    f.write(json.dumps(record, **_JSON_KW) + '\n')
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, plan_dir / _EVENTS_FILE)
            _fsync_dir(plan_dir)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    def _write_snapshot(self, plan_id: str, state: PlanState, *, heal: bool=False) -> PlanSnapshot:
        plan_dir = self._plan_dir(plan_id)
        plan_dir.mkdir(parents=True, exist_ok=True)
        data = to_primitive(state.data)
        applied = dict(state.applied_actions)
        checksum = PayloadChecksum.digest({'plan_id': plan_id, 'sequence': state.version,
                                           'data': data, 'applied_actions': applied})
        body = {'plan_id': plan_id, 'sequence': state.version, 'data': data,
                'applied_actions': applied, 'created_at': iso_z(ensure_utc(self._clock())),
                'checksum': checksum}
        snap_path = plan_dir / _SNAPSHOT_FILE
        prev_path = plan_dir / _PREV_SNAPSHOT_FILE
        fd, tmp = tempfile.mkstemp(prefix=_SNAPSHOT_TMP_PREFIX, dir=str(plan_dir))
        tmp_path = Path(tmp)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                f.write(json.dumps(body, **_JSON_KW))
                f.flush()
                os.fsync(f.fileno())
            if heal:
                for stale in (snap_path, prev_path):
                    if stale.exists():
                        stale.unlink()
            elif snap_path.exists():
                os.replace(snap_path, prev_path)
            os.replace(tmp_path, snap_path)
            _fsync_dir(plan_dir)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
        return PlanSnapshot(plan_id, state.version, deepcopy(data), dict(applied),
                            parse_iso_z(body['created_at']))

    # ----------------------------------------------------------------- reads

    def _read_valid_events(self, plan_id: str) -> tuple[list[PlanEvent], bool]:
        path = self._plan_dir(plan_id) / _EVENTS_FILE
        if not path.exists():
            return [], False
        events: list[PlanEvent] = []
        valid_lines = 0
        lines = path.read_text(encoding='utf-8').splitlines()
        for line in lines:
            try:
                row = json.loads(line)
                event = PlanEvent(plan_id, int(row['sequence']), str(row['event_type']),
                                  dict(row['payload']), str(row['action_id']),
                                  parse_iso_z(str(row['occurred_at'])))
                expected = events[-1].sequence + 1 if events else 1
                digest_ok = _event_digest(event) == str(row.get('checksum', ''))
                if event.sequence != expected or not digest_ok:
                    raise ValidationError('event checksum or sequence mismatch')
            except (KeyError, TypeError, ValueError, ValidationError, json.JSONDecodeError):
                break
            events.append(event)
            valid_lines += 1
        return events, valid_lines < len(lines)

    def _load_snapshot(self, plan_id: str) -> tuple[PlanSnapshot | None, int, str | None]:
        for name in (_SNAPSHOT_FILE, _PREV_SNAPSHOT_FILE):
            path = self._plan_dir(plan_id) / name
            if not path.exists():
                continue
            try:
                row = json.loads(path.read_text(encoding='utf-8'))
                if str(row.get('plan_id')) != plan_id:
                    raise ValidationError('snapshot plan mismatch')
                sequence = int(row['sequence'])
                data = row['data']
                applied_raw = row['applied_actions']
                if not isinstance(data, dict) or not isinstance(applied_raw, dict):
                    raise ValidationError('snapshot body invalid')
                applied = {str(k): int(v) for k, v in applied_raw.items()}
                if sequence < 0 or any(v < 1 or v > sequence for v in applied.values()):
                    raise ValidationError('snapshot sequence invalid')
                checksum = PayloadChecksum.digest({'plan_id': plan_id, 'sequence': sequence,
                                                   'data': data, 'applied_actions': applied})
                if checksum != str(row.get('checksum', '')):
                    raise ValidationError('snapshot checksum mismatch')
                created_at = parse_iso_z(str(row['created_at'])) if row.get('created_at') else None
                return PlanSnapshot(plan_id, sequence, data, applied, created_at), 0, name
            except (KeyError, TypeError, ValueError, ValidationError, json.JSONDecodeError):
                continue
        discarded = sum(1 for name in (_SNAPSHOT_FILE, _PREV_SNAPSHOT_FILE)
                        if (self._plan_dir(plan_id) / name).exists())
        return None, discarded, None

    def _scan_plan_ids(self) -> tuple[str, ...]:
        if not self._dir.exists():
            return ()
        found = []
        for child in self._dir.iterdir():
            if not child.is_dir() or not _PLAN_ID_RE.match(child.name):
                continue
            if any((child / name).exists() for name in
                   (_EVENTS_FILE, _SNAPSHOT_FILE, _PREV_SNAPSHOT_FILE)):
                found.append(child.name)
        return tuple(sorted(found))

    # ---------------------------------------------------------------- folds

    def _fold(self, data: dict[str, object], applied: dict[str, int],
              events: list[PlanEvent]) -> tuple[dict[str, object], dict[str, int]]:
        for event in events:
            result = self._reducer(deepcopy(data), event)
            if not isinstance(result, dict):
                raise ValidationError('plan reducer must return a mapping')
            data = result
            applied[event.action_id] = event.sequence
        return data, applied

    def _resync(self, plan_id: str) -> None:
        events, truncated = self._read_valid_events(plan_id)
        if truncated:
            self._rewrite_events(plan_id, events)
        disk_version = events[-1].sequence if events else 0
        state = self._states.get(plan_id)
        if state is None:
            if disk_version == 0:
                snap, _, _ = self._load_snapshot(plan_id)
                if snap is None:
                    return
                self._states[plan_id] = PlanState(snap.sequence, deepcopy(snap.data),
                                                  dict(snap.applied_actions))
                return
            snap, _, _ = self._load_snapshot(plan_id)
            if snap is not None and snap.sequence > disk_version:
                # verified snapshot beyond the surviving log: keep it as truth
                self._states[plan_id] = PlanState(snap.sequence, deepcopy(snap.data),
                                                  dict(snap.applied_actions))
                self._ahead.add(plan_id)
                return
            base = snap.sequence if snap else 0
            data = deepcopy(snap.data) if snap else {}
            applied = dict(snap.applied_actions) if snap else {}
            data, applied = self._fold(data, applied, events[base:])
            self._states[plan_id] = PlanState(disk_version, data, applied)
            return
        if disk_version < state.version:
            if plan_id in self._ahead:
                return  # snapshot-backed plan: reads are safe, appends fail closed
            raise ConflictError(f'event log for plan {plan_id} lost committed events '
                                f'(disk {disk_version}, memory {state.version})')
        if disk_version > state.version:
            data, applied = self._fold(deepcopy(state.data), dict(state.applied_actions),
                                       events[state.version:])
            self._states[plan_id] = PlanState(disk_version, data, applied)
        self._ahead.discard(plan_id)

    def recover(self) -> RecoveryReport:
        """Rebuild every plan from disk. Safe to call repeatedly.

        State is rebuilt from a snapshot plus later events rather than folded
        on top of live state, so a second recovery cannot double-apply
        anything. The reducer is the only thing invoked; business-effect
        callbacks never run here.
        """
        with self._lock:
            with self._exclusive():
                rebuilt: dict[str, PlanState] = {}
                ahead: set[str] = set()
                rows = []
                for plan_id in self._scan_plan_ids():
                    events, truncated = self._read_valid_events(plan_id)
                    if truncated:
                        self._rewrite_events(plan_id, events)
                    snap, discarded, _source = self._load_snapshot(plan_id)
                    base = snap.sequence if snap else 0
                    data = deepcopy(snap.data) if snap else {}
                    applied = dict(snap.applied_actions) if snap else {}
                    replay = events[base:]
                    data, applied = self._fold(data, applied, replay)
                    disk_version = events[-1].sequence if events else 0
                    is_ahead = bool(snap and snap.sequence > disk_version)
                    if is_ahead:
                        ahead.add(plan_id)
                    version = max(base, disk_version)
                    state = PlanState(version, data, applied)
                    rebuilt[plan_id] = state
                    if discarded > 0 or version - base >= self._snapshot_every:
                        self._write_snapshot(plan_id, state, heal=discarded > 0)
                    rows.append(PlanRecovery(plan_id, version, base, len(replay),
                                             discarded, int(truncated), is_ahead))
                self._states = rebuilt
                self._ahead = ahead
                report = RecoveryReport(tuple(rows))
                self.last_recovery = report
                return report

    # -------------------------------------------------------------- commands

    def execute(self, plan_id: str, action_id: str, expected_version: int,
                event_type: str, payload: dict[str, object] | None=None,
                occurred_at: datetime | None=None) -> CommandOutcome:
        """Append one plan change.

        Stale ``expected_version`` raises :class:`ConflictError`. Retrying a
        command with an already applied ``action_id`` is a no-op: no event is
        appended and the effect handler is not invoked again. When the effect
        handler raises, the event remains durable and the action stays
        applied; retry the side effect out-of-band rather than by resending
        the command.
        """
        _validate_identifier(plan_id, 'plan_id')
        if not isinstance(action_id, str) or not action_id.strip():
            raise ValidationError('action_id required')
        if not isinstance(event_type, str) or not event_type.strip():
            raise ValidationError('event_type required')
        if not isinstance(expected_version, int) or expected_version < 0:
            raise ValidationError('expected_version must be a non-negative integer')
        if payload is not None and not isinstance(payload, dict):
            raise ValidationError('payload must be a mapping')

        fresh_event: PlanEvent | None = None
        fresh_state: PlanState | None = None
        with self._lock:
            with self._exclusive():
                self._resync(plan_id)
                state = self._states.get(plan_id) or PlanState(0, {}, {})
                if action_id in state.applied_actions:
                    return CommandOutcome(plan_id, state.applied_actions[action_id],
                                          state.version, True, state.copied())
                if plan_id in self._ahead:
                    raise ConflictError(
                        f'plan {plan_id} snapshot is ahead of its event log; '
                        f'cannot append until the log is repaired')
                if state.version != expected_version:
                    raise ConflictError(
                        f'plan {plan_id} version conflict: expected {expected_version}, '
                        f'actual {state.version}')
                event = PlanEvent(plan_id, state.version + 1, event_type,
                                  to_primitive(payload or {}), action_id,
                                  ensure_utc(occurred_at or self._clock()))
                self._append_event(event)
                new_data = self._reducer(deepcopy(state.data), event)
                if not isinstance(new_data, dict):
                    raise ValidationError('plan reducer must return a mapping')
                new_applied = dict(state.applied_actions)
                new_applied[action_id] = event.sequence
                new_state = PlanState(event.sequence, new_data, new_applied)
                self._states[plan_id] = new_state
                if event.sequence % self._snapshot_every == 0:
                    self._write_snapshot(plan_id, new_state)
                fresh_event = event
                fresh_state = new_state
            if self._effect_handler is not None:
                self._effect_handler(fresh_event, deepcopy(fresh_state.data))
            return CommandOutcome(fresh_event.plan_id, fresh_event.sequence,
                                  fresh_event.sequence, False, fresh_state.copied())

    def snapshot_now(self, plan_id: str) -> PlanSnapshot:
        _validate_identifier(plan_id, 'plan_id')
        with self._lock:
            with self._exclusive():
                self._resync(plan_id)
                state = self._states.get(plan_id)
                if state is None:
                    raise ConflictError(f'unknown plan {plan_id}')
                return self._write_snapshot(plan_id, state)

    # ------------------------------------------------------------- queries

    def state(self, plan_id: str) -> PlanState | None:
        _validate_identifier(plan_id, 'plan_id')
        with self._lock:
            with self._exclusive():
                self._resync(plan_id)
                state = self._states.get(plan_id)
                return state.copied() if state else None

    def version(self, plan_id: str) -> int:
        state = self.state(plan_id)
        return state.version if state else 0

    def plan_ids(self) -> tuple[str, ...]:
        with self._lock:
            with self._exclusive():
                for plan_id in self._scan_plan_ids():
                    self._resync(plan_id)
                return tuple(sorted(self._states))

    def read_events(self, plan_id: str, after_sequence: int=0) -> tuple[PlanEvent, ...]:
        _validate_identifier(plan_id, 'plan_id')
        if after_sequence < 0:
            raise ValidationError('after_sequence cannot be negative')
        with self._lock:
            with self._exclusive():
                self._resync(plan_id)
                events, truncated = self._read_valid_events(plan_id)
                if truncated:
                    raise ConflictError(f'plan {plan_id} event log has a corrupt tail')
                return tuple(e for e in events if e.sequence > after_sequence)
