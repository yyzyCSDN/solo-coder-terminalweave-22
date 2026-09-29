import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from terminalweave.core.errors import (
    ConflictError,
    NotFoundError,
    StateTransitionError,
    ValidationError,
)
from terminalweave.persistence.action_ledger import ActionLedger, action_key
from terminalweave.persistence.durable_event_store import DurableEventStore
from terminalweave.persistence.durable_snapshot_store import DurableSnapshotStore
from terminalweave.persistence.plan_store import (
    ActionContext,
    ActionExecutionError,
    PLAN_CREATED,
    PLAN_ITEM_ADDED,
    RestartablePlanStore,
)

D = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _open(tmp_path, **kw):
    return RestartablePlanStore(str(tmp_path / 'plans'), snapshot_interval=kw.pop(
        'snapshot_interval', 5), **kw)


def test_append_and_recover_without_snapshot(tmp_path):
    store = _open(tmp_path)
    store.create_plan('p1', 'Berth rotation')
    r1 = store.update_fields('p1', 1, {'vessel': 'MV Star'})
    store.add_item('p1', r1.version, 'job-1', {'crane': 'QC3'})
    assert store.version('p1') == 3

    # new process: no snapshot yet, full replay from events
    store2 = _open(tmp_path)
    report = store2.recover('p1')
    assert not report.snapshot_used
    assert report.replayed_events == 3
    assert report.recovered_sequence == 3
    state = store2.state('p1')
    assert state['name'] == 'Berth rotation'
    assert state['fields'] == {'vessel': 'MV Star'}
    assert state['items']['job-1']['data'] == {'crane': 'QC3'}
    assert state['status'] == 'draft'


def test_periodic_snapshot_then_restart_uses_and_extends_it(tmp_path):
    store = _open(tmp_path, snapshot_interval=5)
    store.create_plan('p1', 'Plan')  # seq 1
    for i in range(1, 4):
        store.add_item('p1', store.version('p1'), f'job-{i}', {'n': i})
    assert store.version('p1') == 4
    assert store._snapshots.get('p1') is None
    r = store.add_item('p1', store.version('p1'), 'job-4', {'n': 4})  # seq 5
    assert r.snapshot_taken is not None and r.snapshot_taken.sequence == 5

    store2 = _open(tmp_path, snapshot_interval=5)
    report = store2.recover('p1')
    assert report.snapshot_used
    assert report.snapshot_sequence == 5
    assert report.replayed_events == 0
    assert len(store2.state('p1')['items']) == 4

    # an event appended after the snapshot is folded on top of it
    store2.add_item('p1', store2.version('p1'), 'job-5', {'n': 5})
    store3 = _open(tmp_path, snapshot_interval=5)
    report = store3.recover('p1')
    assert report.snapshot_used
    assert report.replayed_events == 1
    assert len(store3.state('p1')['items']) == 5


def test_stale_version_rejected_under_concurrent_writes(tmp_path):
    store = _open(tmp_path)
    store.create_plan('p1', 'Plan')

    # two threads both believe they are at version 1; only one wins
    import threading
    errors = []

    def add(item_id):
        try:
            store.add_item('p1', 1, item_id, {})
        except ConflictError as exc:
            errors.append(exc)

    t1 = threading.Thread(target=add, args=('a',))
    t2 = threading.Thread(target=add, args=('b',))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert len(errors) == 1
    assert store.version('p1') == 2


def test_corrupt_snapshot_is_rebuilt_from_events(tmp_path):
    store = _open(tmp_path, snapshot_interval=5)
    store.create_plan('p1', 'Plan')  # seq 1
    for i in range(1, 4):
        store.add_item('p1', store.version('p1'), f'job-{i}', {'n': i})
    assert store._snapshots.get('p1') is None
    store.add_item('p1', store.version('p1'), 'job-4', {'n': 4})  # seq 5
    snap = store._snapshots._path('p1')
    assert snap.exists()

    # smash the snapshot bytes; recovery must quarantine and replay
    snap.write_text('{"sequence": 5, "state": {"totally": "wrong"')
    store2 = _open(tmp_path)
    report = store2.recover('p1')
    assert report.corrupt_snapshot
    assert report.snapshot_rebuilt
    assert not report.snapshot_used
    assert report.replayed_events == 5
    assert len(store2.state('p1')['items']) == 4

    # a valid, fresh snapshot now exists
    fresh = store2._snapshots.get('p1')
    assert fresh is not None and fresh.sequence == 5
    corrupt_files = list((tmp_path / 'plans' / 'snapshots').glob('*.corrupt-*'))
    assert len(corrupt_files) == 1


def test_snapshot_ahead_of_events_is_ignored(tmp_path):
    store = _open(tmp_path, snapshot_interval=5)
    store.create_plan('p1', 'Plan')
    for i in range(1, 4):
        store.add_item('p1', store.version('p1'), f'job-{i}', {})
    # hand-write a snapshot claiming sequence 99 while log has only 3 events
    state = {'plan_id': 'p1', 'name': 'Plan', 'status': 'draft',
             'revision': 99, 'fields': {}, 'items': {}}
    with pytest.raises(ConflictError):
        store._snapshots.put('p1', 99, state, max_sequence=store.version('p1'))
    store._snapshots.put('p1', 99, state)  # as if it came from a stray writer

    store2 = _open(tmp_path)
    report = store2.recover('p1')
    assert not report.snapshot_used
    assert report.recovered_sequence == 4
    assert report.snapshot_rebuilt
    assert store2.state('p1')['revision'] == 4


def test_business_actions_not_duplicated_on_repeated_recovery(tmp_path):
    calls = []

    def handler(ctx: ActionContext):
        # deliberately idempotent on ctx.key; record what actually ran
        calls.append((ctx.key, ctx.payload['item_id']))

    store = _open(tmp_path, snapshot_interval=100)
    store.register_action(PLAN_ITEM_ADDED, handler)
    store.create_plan('p1', 'Plan')
    store.add_item('p1', 1, 'job-1', {})
    store.add_item('p1', 2, 'job-2', {})
    assert len(calls) == 2

    # first restart: actions already completed -> none redispatched
    store2 = _open(tmp_path, snapshot_interval=100)
    store2.register_action(PLAN_ITEM_ADDED, handler)
    report = store2.recover('p1')
    assert report.actions_skipped_completed == 2
    assert report.actions_dispatched == ()
    assert len(calls) == 2

    # second restart with a brand-new handler instance: still no duplicates
    store3 = _open(tmp_path, snapshot_interval=100)
    store3.register_action(PLAN_ITEM_ADDED, handler)
    store3.recover('p1')
    assert len(calls) == 2


def test_interrupted_action_is_redispatched_once(tmp_path):
    runs = []

    def flaky(ctx):
        runs.append(ctx.key)
        if len(runs) == 1:
            raise RuntimeError('side effect lost to crash')

    store = _open(tmp_path, snapshot_interval=100)
    store.register_action(PLAN_ITEM_ADDED, flaky)
    store.create_plan('p1', 'Plan')
    with pytest.raises(ActionExecutionError):
        store.add_item('p1', 1, 'job-1', {})
    assert store._ledger.status_of(action_key('p1', 2)) == 'failed'
    # event is durable even though the action failed
    assert store.version('p1') == 2

    store2 = _open(tmp_path, snapshot_interval=100)
    store2.register_action(PLAN_ITEM_ADDED, flaky)
    report = store2.recover('p1')
    assert report.actions_dispatched == (action_key('p1', 2),)
    # a further recovery does not run it again
    store3 = _open(tmp_path, snapshot_interval=100)
    store3.register_action(PLAN_ITEM_ADDED, flaky)
    report = store3.recover('p1')
    assert report.actions_dispatched == ()
    assert len(runs) == 2


def test_command_id_makes_append_idempotent(tmp_path):
    store = _open(tmp_path)
    store.create_plan('p1', 'Plan')
    first = store.add_item('p1', 1, 'job-1', {}, command_id='cmd-77')
    again = store.add_item('p1', 1, 'job-1', {}, command_id='cmd-77')
    assert again.deduped
    assert again.version == first.version
    assert store.version('p1') == 2


def test_optimistic_guard_on_reducer_conflict(tmp_path):
    store = _open(tmp_path)
    store.create_plan('p1', 'Plan')
    # unknown item: reducer rejects before anything is persisted
    with pytest.raises(NotFoundError):
        store.change_item_status('p1', 1, 'job-x', 'done')
    assert store.version('p1') == 1

    # stale old_status on a status change is a concurrency conflict
    store.add_item('p1', 1, 'job-1', {}, status='planned')
    v = store.version('p1')
    store.change_item_status('p1', v, 'job-1', 'started', old_status='planned')
    with pytest.raises(ConflictError):
        store.change_item_status(
            'p1', store.version('p1'), 'job-1', 'done', old_status='planned')


def test_torn_event_tail_is_repaired_on_recovery(tmp_path):
    store = _open(tmp_path)
    store.create_plan('p1', 'Plan')
    store.add_item('p1', 1, 'job-1', {})
    log = store._events._path('p1')
    good = log.read_bytes()
    store.add_item('p1', 2, 'job-2', {})
    full = log.read_bytes()
    # simulate a crash halfway through appending event 3: no trailing newline
    cut = len(good) + (len(full) - len(good)) // 2
    log.write_bytes(full[:cut])

    store2 = _open(tmp_path)
    report = store2.recover('p1')
    assert report.torn_events_repaired == 1
    assert report.recovered_sequence == 2
    # store is writable again and continues from sequence 3
    r = store2.add_item('p1', 2, 'job-2b', {})
    assert r.version == 3


def test_snapshot_missing_falls_back_to_all_events(tmp_path):
    store = _open(tmp_path, snapshot_interval=2)
    store.create_plan('p1', 'Plan')
    store.add_item('p1', 1, 'job-1', {})
    store.add_item('p1', 2, 'job-2', {})
    assert store._snapshots.get('p1') is not None
    store._snapshots._path('p1').unlink()

    store2 = _open(tmp_path, snapshot_interval=2)
    report = store2.recover('p1')
    assert not report.snapshot_used
    assert report.replayed_events == 3
    assert report.snapshot_rebuilt


def test_frozen_plan_rejects_mutating_events_and_reducer_replay(tmp_path):
    store = _open(tmp_path)
    store.create_plan('p1', 'Plan')
    store.freeze('p1', 1)
    with pytest.raises(StateTransitionError):
        store.add_item('p1', 2, 'late', {})


def test_pending_action_inside_snapshot_is_redispatched(tmp_path):
    """Snapshot taken while an action is still pending must not hide it.

    Simulate a crash after event+snapshot are durable but before the action
    completed: hand-write a pending ledger entry for an event already folded
    into the snapshot, then recover -- the action runs exactly once.
    """
    runs = []

    def handler(ctx: ActionContext):
        runs.append(ctx.key)

    store = _open(tmp_path, snapshot_interval=2)
    store.register_action(PLAN_ITEM_ADDED, handler)
    store.create_plan('p1', 'Plan')            # seq 1
    store.add_item('p1', 1, 'job-1', {})       # seq 2 -> snapshot
    assert runs == [action_key('p1', 2)]

    # pretend the action never completed despite the snapshot
    store._ledger.mark_pending(action_key('p1', 2))

    store2 = _open(tmp_path, snapshot_interval=2)
    store2.register_action(PLAN_ITEM_ADDED, handler)
    report = store2.recover('p1', dispatch=True)
    assert report.snapshot_used
    assert report.replayed_events == 0
    assert report.actions_dispatched == (action_key('p1', 2),)
    assert runs == [action_key('p1', 2), action_key('p1', 2)]

    # second recovery: now completed, never run again
    store3 = _open(tmp_path, snapshot_interval=2)
    store3.register_action(PLAN_ITEM_ADDED, handler)
    store3.recover('p1')
    assert len(runs) == 2


def test_event_checksum_tampering_detected(tmp_path):
    store = DurableEventStore(tmp_path / 'events')
    store.append('p1', 0, PLAN_CREATED, {'name': 'Plan'})
    log = Path(store._path('p1'))
    lines = log.read_text().splitlines()
    record = json.loads(lines[0])
    record['payload'] = {'name': 'Forged'}
    log.write_text(json.dumps(record) + '\n')
    with pytest.raises(ValidationError):
        store.load('p1', repair_tail=False)


def test_stream_id_path_traversal_rejected(tmp_path):
    store = DurableEventStore(tmp_path / 'events')
    with pytest.raises(ValidationError):
        store.append('../escape', 0, PLAN_CREATED, {'name': 'x'})


def test_action_ledger_compaction(tmp_path):
    ledger = ActionLedger(tmp_path)
    ledger.mark_pending('p:1'); ledger.mark_completed('p:1')
    ledger.mark_pending('p:2')
    # without a snapshot boundary nothing completed is dropped (only the
    # superseded pending->completed transition of p:1 collapses)
    assert ledger.compact() == 1
    assert ledger.status_of('p:1') == 'completed'
    assert ledger.status_of('p:2') == 'pending'
    # a snapshot at seq 1 retires p:1 but never the pending p:2
    removed = ledger.compact(drop_completed_up_to={'p': 1})
    assert removed == 1
    assert ledger.status_of('p:1') is None
    assert ledger.status_of('p:2') == 'pending'
    # reloaded ledger reflects compaction
    ledger2 = ActionLedger(tmp_path)
    assert ledger2.status_of('p:1') is None
    assert ledger2.status_of('p:2') == 'pending'
