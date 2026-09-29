from __future__ import annotations
from copy import deepcopy
from dataclasses import dataclass
from threading import RLock
from typing import Callable
from terminalweave.core.clock import utc_now
from terminalweave.core.errors import (
    ConflictError,
    NotFoundError,
    StateTransitionError,
    ValidationError,
)
from terminalweave.persistence.action_ledger import ActionLedger, action_key
from terminalweave.persistence.durable_event_store import (
    DurableEventStore,
    DurableStoredEvent,
)
from terminalweave.persistence.durable_snapshot_store import (
    DurableSnapshot,
    DurableSnapshotStore,
)

# --- built-in plan event types -------------------------------------------------

PLAN_CREATED = 'plan.created'
PLAN_FIELDS_UPDATED = 'plan.fields_updated'
PLAN_ITEM_ADDED = 'plan.item_added'
PLAN_ITEM_STATUS_CHANGED = 'plan.item_status_changed'
PLAN_FROZEN = 'plan.frozen'
PLAN_CLOSED = 'plan.closed'


@dataclass(frozen=True)
class ActionContext:
    """Passed to business-action handlers; ``key`` is the idempotency key."""
    key: str
    plan_id: str
    sequence: int
    event_type: str
    payload: dict[str, object]


@dataclass(frozen=True)
class AppendResult:
    event: DurableStoredEvent
    state: dict[str, object]
    version: int
    deduped: bool = False
    action_executed: bool = False
    snapshot_taken: DurableSnapshot | None = None


@dataclass
class RecoveryReport:
    plan_id: str
    snapshot_used: bool = False
    corrupt_snapshot: bool = False
    snapshot_rebuilt: bool = False
    torn_events_repaired: int = 0
    snapshot_sequence: int = 0
    recovered_sequence: int = 0
    replayed_events: int = 0
    actions_dispatched: tuple[str, ...] = ()
    actions_skipped_completed: int = 0


ActionHandler = Callable[[ActionContext], None]
Reducer = Callable[[dict | None, DurableStoredEvent], dict]


class ActionExecutionError(RuntimeError):
    """The event was durably stored but its business action raised.

    Recovery (or an explicit retry) will redispatch the action at most once
    more per restart; handlers must be idempotent on ``context.key``.
    """


# --- default aggregate reducer -------------------------------------------------

def default_plan_reducer(state: dict[str, object] | None,
                         event: DurableStoredEvent) -> dict[str, object]:
    """Fold one plan event onto the aggregate state.

    With ``state is None`` the first event must be ``plan.created``. The
    reducer is deliberately strict: replaying an invalid transition is a
    sign of event-log corruption, not something to ignore.
    """
    payload = event.payload
    if event.event_type == PLAN_CREATED:
        if state is not None:
            raise ValidationError('plan.created can only seed an empty stream')
        name = payload.get('name')
        if not isinstance(name, str) or not name.strip():
            raise ValidationError('plan name required')
        return {
            'plan_id': event.stream_id,
            'name': name,
            'status': 'draft',
            'revision': event.sequence,
            'fields': {},
            'items': {},
        }
    if state is None:
        raise ValidationError(
            f'{event.event_type} applied before plan.created')
    next_state = deepcopy(state)

    if event.event_type == PLAN_FIELDS_UPDATED:
        if next_state['status'] != 'draft':
            raise StateTransitionError('only draft plans accept field updates')
        fields = payload.get('fields')
        if not isinstance(fields, dict) or not fields:
            raise ValidationError('fields mapping required')
        next_state['fields'].update(deepcopy(fields))
    elif event.event_type == PLAN_ITEM_ADDED:
        if next_state['status'] != 'draft':
            raise StateTransitionError('only draft plans accept new items')
        item_id = payload.get('item_id')
        if not isinstance(item_id, str) or not item_id:
            raise ValidationError('item_id required')
        if item_id in next_state['items']:
            raise ConflictError(f'item {item_id} already exists')
        data = payload.get('data', {})
        if not isinstance(data, dict):
            raise ValidationError('item data must be a mapping')
        next_state['items'][item_id] = {
            'status': payload.get('status', 'pending'),
            'data': deepcopy(data),
        }
    elif event.event_type == PLAN_ITEM_STATUS_CHANGED:
        item_id = payload.get('item_id')
        new_status = payload.get('new_status')
        if not isinstance(item_id, str) or not isinstance(new_status, str):
            raise ValidationError('item_id and new_status required')
        item = next_state['items'].get(item_id)
        if item is None:
            raise NotFoundError(f'item {item_id} does not exist')
        old_status = payload.get('old_status')
        if old_status is not None and item['status'] != old_status:
            raise ConflictError(
                f'item {item_id} expected status {old_status}, '
                f'actual {item["status"]}')
        item['status'] = new_status
    elif event.event_type == PLAN_FROZEN:
        if next_state['status'] != 'draft':
            raise StateTransitionError('plan is not draft')
        next_state['status'] = 'frozen'
    elif event.event_type == PLAN_CLOSED:
        if next_state['status'] == 'closed':
            raise StateTransitionError('plan already closed')
        next_state['status'] = 'closed'
    else:
        # unknown event types are carried without interpretation so custom
        # reducers / applications can extend the vocabulary
        extras = next_state.setdefault('extras', [])
        extras.append({'sequence': event.sequence,
                       'event_type': event.event_type,
                       'payload': deepcopy(payload)})
    next_state['revision'] = event.sequence
    return next_state


# --- store ---------------------------------------------------------------------

class RestartablePlanStore:
    """Event-sourced, snapshot-backed plan storage that survives restarts.

    - Key plan changes are appended as strictly ordered, checksummed events
      (one log per plan); appends carry the caller's expected version and a
      stale version raises :class:`ConflictError`.
    - Snapshots of folded state are written periodically (every
      ``snapshot_interval`` events) and atomically.
    - On :meth:`recover`, the newest snapshot is validated; a corrupt,
      missing, or future-dated snapshot is discarded and state is rebuilt by
      replaying events, then a fresh snapshot is written.
    - Business actions attached to event types run at least once and are
      tracked in a durable action ledger: a completed action is never run
      again on a repeated recovery; an action interrupted by a crash (still
      ``pending``) is redispatched.
    """

    def __init__(self, directory: str, *, snapshot_interval: int = 20,
                 reducer: Reducer = None):
        if snapshot_interval < 1:
            raise ValidationError('snapshot_interval must be positive')
        self._events = DurableEventStore(directory)
        self._snapshots = DurableSnapshotStore(directory)
        self._ledger = ActionLedger(directory)
        self._snapshot_interval = snapshot_interval
        self._reducer: Reducer = reducer or default_plan_reducer
        self._handlers: dict[str, ActionHandler] = {}
        self._states: dict[str, dict[str, object]] = {}
        self._lock = RLock()

    # -- handler registration -------------------------------------------------

    def register_action(self, event_type: str, handler: ActionHandler) -> None:
        if not event_type or not callable(handler):
            raise ValidationError('event_type and callable handler required')
        self._handlers[event_type] = handler

    # -- reads ----------------------------------------------------------------

    def plans(self) -> tuple[str, ...]:
        return self._events.all_streams()

    def version(self, plan_id: str) -> int:
        return self._events.current_sequence(plan_id)

    def state(self, plan_id: str) -> dict[str, object]:
        with self._lock:
            cached = self._states.get(plan_id)
            if cached is not None:
                return deepcopy(cached)
            events = self._events.read(plan_id)
            if not events:
                raise NotFoundError(f'unknown plan {plan_id}')
            snapshot = self._snapshots.get(plan_id)
            state = self._fold(events, snapshot)
            self._states[plan_id] = state
            return deepcopy(state)

    # -- writes ---------------------------------------------------------------

    def append(self, plan_id: str, expected_version: int, event_type: str,
               payload: dict[str, object], *, command_id: str | None = None,
               occurred_at=None) -> AppendResult:
        with self._lock:
            events, _repaired = self._events.load(plan_id)
            if command_id is not None:
                for previous in events:
                    if previous.command_id == command_id:
                        state = self._fold_to(plan_id, events)
                        return AppendResult(previous, state,
                                            previous.sequence, deduped=True)
            cached = self._states.get(plan_id)
            base_state = (deepcopy(cached) if cached is not None
                          else self._fold(events, self._snapshots.get(plan_id))
                          if events else None)
            # validate the transition against the reducer before persisting
            trial = DurableStoredEvent(plan_id, len(events) + 1, event_type,
                                       payload,
                                       occurred_at or utc_now(), command_id, '')
            self._reducer(deepcopy(base_state), trial)

            event = self._events.append(plan_id, expected_version, event_type,
                                        payload, occurred_at=occurred_at,
                                        command_id=command_id)
            state = self._reducer(deepcopy(base_state), event)
            self._states[plan_id] = state

            action_executed = False
            if event.event_type in self._handlers:
                self._run_action(event)
                action_executed = True

            snapshot_taken = None
            if self._snapshot_interval and event.sequence % self._snapshot_interval == 0:
                snapshot_taken = self._save_snapshot(plan_id, state,
                                                     event.sequence)
            return AppendResult(event, deepcopy(state), event.sequence,
                                action_executed=action_executed,
                                snapshot_taken=snapshot_taken)

    # convenience commands ----------------------------------------------------

    def create_plan(self, plan_id: str, name: str, *,
                    command_id: str | None = None) -> AppendResult:
        return self.append(plan_id, 0, PLAN_CREATED, {'name': name},
                           command_id=command_id)

    def update_fields(self, plan_id: str, expected_version: int,
                      fields: dict[str, object], *,
                      command_id: str | None = None) -> AppendResult:
        return self.append(plan_id, expected_version, PLAN_FIELDS_UPDATED,
                           {'fields': fields}, command_id=command_id)

    def add_item(self, plan_id: str, expected_version: int, item_id: str,
                 data: dict[str, object] | None = None, *, status: str = 'pending',
                 command_id: str | None = None) -> AppendResult:
        return self.append(plan_id, expected_version, PLAN_ITEM_ADDED,
                           {'item_id': item_id, 'data': data or {},
                            'status': status}, command_id=command_id)

    def change_item_status(self, plan_id: str, expected_version: int,
                           item_id: str, new_status: str, *,
                           old_status: str | None = None,
                           command_id: str | None = None) -> AppendResult:
        return self.append(plan_id, expected_version, PLAN_ITEM_STATUS_CHANGED,
                           {'item_id': item_id, 'new_status': new_status,
                            'old_status': old_status},
                           command_id=command_id)

    def freeze(self, plan_id: str, expected_version: int, *,
               command_id: str | None = None) -> AppendResult:
        return self.append(plan_id, expected_version, PLAN_FROZEN, {},
                           command_id=command_id)

    # -- snapshots ------------------------------------------------------------

    def save_snapshot(self, plan_id: str) -> DurableSnapshot | None:
        with self._lock:
            state = self._states.get(plan_id)
            if state is None:
                events = self._events.read(plan_id)
                if not events:
                    return None
                state = self._fold_to(plan_id, events)
            return self._save_snapshot(plan_id, state,
                                       self._events.current_sequence(plan_id))

    def _save_snapshot(self, plan_id: str, state: dict[str, object],
                       sequence: int) -> DurableSnapshot:
        snapshot = self._snapshots.put(
            plan_id, sequence, state,
            max_sequence=self._events.current_sequence(plan_id))
        # completed action records at or below the snapshot boundary can be
        # dropped: their events are folded into the durable snapshot, so the
        # actions are never redispatched even after a full replay. Older
        # completed records (snapshot previously missing/lost) are retained.
        self._ledger.compact(drop_completed_up_to={plan_id: sequence})
        return snapshot

    # -- recovery -------------------------------------------------------------

    def recover(self, plan_id: str, *, dispatch: bool = True) -> RecoveryReport:
        """Restore one plan to a consistent state after a process restart.

        Validates the snapshot, replays whatever events it is missing (or all
        events when the snapshot is corrupt/ahead), rebuilds the snapshot, and
        dispatches any business actions that never completed — without ever
        repeating a completed one.
        """
        with self._lock:
            report = RecoveryReport(plan_id=plan_id)
            snap_path = self._snapshots._path(plan_id)
            snapshot_present = snap_path.exists()
            events, torn = self._events.load(plan_id)
            if not events:
                raise NotFoundError(f'unknown plan {plan_id}')
            report.recovered_sequence = events[-1].sequence
            report.torn_events_repaired = 1 if torn else 0

            snapshot = self._snapshots.get(plan_id) if snapshot_present else None
            if snapshot_present and snapshot is None:
                report.corrupt_snapshot = True
            snapshot_ahead = (
                snapshot is not None and snapshot.sequence > events[-1].sequence)
            if snapshot_ahead:
                # checksum-valid but newer than the log: unusable, drop it
                self._snapshots.delete(plan_id)
                snapshot = None

            base_state = None
            base_sequence = 0
            if snapshot is not None and 0 < snapshot.sequence <= events[-1].sequence:
                base_state = deepcopy(snapshot.state)
                base_sequence = snapshot.sequence
                report.snapshot_used = True
                report.snapshot_sequence = snapshot.sequence

            state = base_state
            dispatched: list[str] = []
            skipped = 0
            tail = [e for e in events if e.sequence > base_sequence]

            # The snapshot folds events at sequence <= base_sequence, but an
            # action for one of them may still be pending/failed in the ledger
            # if the process died after snapshotting but before completing it.
            # Those actions are not replayed as events, so redispatch them here.
            if dispatch:
                tail_by_key = {action_key(plan_id, e.sequence): e
                               for e in tail if e.event_type in self._handlers}
                for key in self._ledger.pending_keys():
                    pid, _, seq = key.rpartition(':')
                    if pid != plan_id or not seq.isdigit():
                        continue
                    if int(seq) > base_sequence:
                        continue  # handled while replaying the tail
                    if key in tail_by_key:
                        continue
                    event = next((e for e in events if e.sequence == int(seq)),
                                 None)
                    if event is not None and event.event_type in self._handlers:
                        self._run_action(event)
                        dispatched.append(key)

            for event in tail:
                state = self._reducer(state, event)
                if event.event_type not in self._handlers:
                    continue
                key = action_key(plan_id, event.sequence)
                status = self._ledger.status_of(key)
                if status == 'completed':
                    skipped += 1
                    continue
                if dispatch:
                    self._run_action(event)
                    dispatched.append(key)
            report.replayed_events = len(tail)
            report.actions_dispatched = tuple(dispatched)
            report.actions_skipped_completed = skipped

            if state is None:
                raise ValidationError(f'plan {plan_id} has no foldable state')
            self._states[plan_id] = state

            need_rebuild = (
                report.corrupt_snapshot
                or not report.snapshot_used
                or (snapshot is not None and snapshot.sequence < events[-1].sequence
                    and events[-1].sequence - snapshot.sequence >= self._snapshot_interval)
            )
            if need_rebuild:
                self._save_snapshot(plan_id, state, events[-1].sequence)
                report.snapshot_rebuilt = True
                report.snapshot_sequence = events[-1].sequence
            return report

    def recover_all(self, *, dispatch: bool = True) -> list[RecoveryReport]:
        reports = []
        for plan_id in self.plans():
            reports.append(self.recover(plan_id, dispatch=dispatch))
        return reports

    # -- internals ------------------------------------------------------------

    def _run_action(self, event: DurableStoredEvent) -> None:
        handler = self._handlers[event.event_type]
        key = action_key(event.stream_id, event.sequence)
        if self._ledger.status_of(key) == 'completed':
            return
        context = ActionContext(key, event.stream_id, event.sequence,
                                event.event_type, dict(event.payload))
        self._ledger.mark_pending(key)
        try:
            handler(context)
        except Exception as exc:  # event stays durable; recovery retries once
            self._ledger.mark_failed(key, repr(exc))
            raise ActionExecutionError(
                f'action for {event.event_type} ({key}) failed: {exc}') from exc
        self._ledger.mark_completed(key)

    def _fold(self, events, snapshot: DurableSnapshot | None
              ) -> dict[str, object]:
        if snapshot is not None and 0 < snapshot.sequence <= events[-1].sequence:
            state = deepcopy(snapshot.state)
            tail = [e for e in events if e.sequence > snapshot.sequence]
        else:
            state = None
            tail = events
        for event in tail:
            state = self._reducer(state, event)
        return state

    def _fold_to(self, plan_id: str, events) -> dict[str, object]:
        state = self._fold(events, self._snapshots.get(plan_id))
        self._states[plan_id] = state
        return state
