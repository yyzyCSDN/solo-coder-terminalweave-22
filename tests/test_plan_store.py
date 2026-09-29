import json
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from terminalweave.core.errors import ConflictError, ValidationError
from terminalweave.persistence.plan_store import (
    DurablePlanStore, default_plan_reducer, PlanEvent,
)

UTC = timezone.utc
D = datetime(2026, 1, 1, tzinfo=UTC)


class raises:
    def __init__(self, exc_type):
        self.exc_type = exc_type

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError(f'{self.exc_type.__name__} not raised')
        return issubclass(exc_type, self.exc_type)


def make_store(tmp, **kwargs):
    path = Path(tempfile.mkdtemp(dir=tmp))
    return DurablePlanStore(path, clock=lambda: D, **kwargs), path


def jobs_reducer(data, event):
    data = dict(data)
    data.setdefault('jobs', {})
    if event.event_type == 'job_scheduled':
        data['jobs'] = dict(data['jobs'])
        data['jobs'][event.payload['job_id']] = event.payload
    elif event.event_type == 'job_cancelled':
        data['jobs'] = {k: v for k, v in data['jobs'].items()
                        if k != event.payload['job_id']}
    return data


def test_stale_version_is_rejected_and_events_stay_ordered(tmp_path):
    store, _ = make_store(tmp_path)
    out = store.execute('PLAN1', 'a1', 0, 'plan_created', {'name': 'vessel-berth'})
    assert out.version == 1 and out.sequence == 1 and not out.duplicate
    with raises(ConflictError):
        store.execute('PLAN1', 'a2', 0, 'job_scheduled', {'job_id': 'J1'})
    out = store.execute('PLAN1', 'a2', 1, 'job_scheduled', {'job_id': 'J1'})
    assert out.version == 2
    events = store.read_events('PLAN1')
    assert [e.sequence for e in events] == [1, 2]
    assert [e.action_id for e in events] == ['a1', 'a2']
    store.close()


def test_restart_recovers_consistent_state(tmp_path):
    _, path = make_store(tmp_path)
    store = DurablePlanStore(path, snapshot_every=3, reducer=jobs_reducer, clock=lambda: D)
    store.execute('P', 'a1', 0, 'plan_created', {'name': 'plan'})
    store.execute('P', 'a2', 1, 'job_scheduled', {'job_id': 'J1'})
    store.execute('P', 'a3', 2, 'job_scheduled', {'job_id': 'J2'})

    restarted = DurablePlanStore(path, snapshot_every=3, reducer=jobs_reducer, clock=lambda: D)
    state = restarted.state('P')
    assert state.version == 3
    assert set(state.data['jobs']) == {'J1', 'J2'}
    assert state.applied_actions == {'a1': 1, 'a2': 2, 'a3': 3}
    restarted.close()


def test_snapshot_written_periodically(tmp_path):
    store, path = make_store(tmp_path, snapshot_every=2, reducer=jobs_reducer)
    assert not (path / 'P' / 'snapshot.json').exists()
    store.execute('P', 'a1', 0, 'plan_created', {})
    assert not (path / 'P' / 'snapshot.json').exists()
    store.execute('P', 'a2', 1, 'job_scheduled', {'job_id': 'J1'})
    snap = json.loads((path / 'P' / 'snapshot.json').read_text())
    assert snap['sequence'] == 2 and snap['applied_actions'] == {'a1': 1, 'a2': 2}
    store.close()


def test_lagging_snapshot_is_backfilled_from_events(tmp_path):
    _, path = make_store(tmp_path, snapshot_every=2, reducer=jobs_reducer)
    store = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer, clock=lambda: D)
    for i in range(1, 5):
        store.execute('P', f'a{i}', i - 1, 'job_scheduled', {'job_id': f'J{i}'})
    # snapshot covers seq 4; delete it so restart falls back to seq 2
    (path / 'P' / 'snapshot.json').unlink()

    restarted = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer, clock=lambda: D)
    recovery = [p for p in restarted.last_recovery.plans if p.plan_id == 'P'][0]
    assert recovery.snapshot_sequence == 2 and recovery.events_replayed == 2
    assert set(restarted.state('P').data['jobs']) == {'J1', 'J2', 'J3', 'J4'}
    assert restarted.state('P').version == 4
    restarted.close()


def test_corrupt_snapshots_discarded_and_rebuilt_from_events(tmp_path):
    _, path = make_store(tmp_path, snapshot_every=2, reducer=jobs_reducer)
    store = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer, clock=lambda: D)
    for i in range(1, 5):
        store.execute('P', f'a{i}', i - 1, 'job_scheduled', {'job_id': f'J{i}'})
    (path / 'P' / 'snapshot.json').write_text('{"not": "a snapshot", broken')
    (path / 'P' / 'snapshot.prev.json').write_text('GARBAGE-NOT-JSON')

    restarted = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer, clock=lambda: D)
    recovery = [p for p in restarted.last_recovery.plans if p.plan_id == 'P'][0]
    assert recovery.snapshots_discarded == 2
    assert recovery.snapshot_sequence == 0 and recovery.events_replayed == 4
    assert set(restarted.state('P').data['jobs']) == {'J1', 'J2', 'J3', 'J4'}
    # heal: a valid snapshot has replaced the corrupt pair
    snap = json.loads((path / 'P' / 'snapshot.json').read_text())
    assert snap['sequence'] == 4
    restarted.close()


def test_snapshot_checksum_tampering_triggers_full_replay(tmp_path):
    _, path = make_store(tmp_path, snapshot_every=2, reducer=jobs_reducer)
    store = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer, clock=lambda: D)
    store.execute('P', 'a1', 0, 'plan_created', {'name': 'real'})
    store.execute('P', 'a2', 1, 'job_scheduled', {'job_id': 'J1'})
    snap_path = path / 'P' / 'snapshot.json'
    body = json.loads(snap_path.read_text())
    body['data']['forged'] = True
    snap_path.write_text(json.dumps(body))

    restarted = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer, clock=lambda: D)
    state = restarted.state('P')
    assert state.version == 2
    assert 'forged' not in state.data
    assert set(state.data['jobs']) == {'J1'}
    restarted.close()


def test_torn_event_tail_is_truncated_and_healed(tmp_path):
    _, path = make_store(tmp_path, snapshot_every=100, reducer=jobs_reducer)
    store = DurablePlanStore(path, snapshot_every=100, reducer=jobs_reducer, clock=lambda: D)
    store.execute('P', 'a1', 0, 'plan_created', {})
    store.execute('P', 'a2', 1, 'job_scheduled', {'job_id': 'J1'})
    events_path = path / 'P' / 'events.jsonl'
    with events_path.open('a') as f:
        f.write('{"sequence": 3, "event_type": "job_scheduled", "partial')

    restarted = DurablePlanStore(path, snapshot_every=100, reducer=jobs_reducer, clock=lambda: D)
    recovery = [p for p in restarted.last_recovery.plans if p.plan_id == 'P'][0]
    assert recovery.events_truncated == 1 and restarted.version('P') == 2
    # healed file now ends exactly at seq 2
    lines = events_path.read_text().splitlines()
    assert len(lines) == 2
    # appending again reuses sequence 3 cleanly
    out = restarted.execute('P', 'a3', 2, 'job_scheduled', {'job_id': 'J2'})
    assert out.version == 3
    restarted.close()


def test_duplicate_action_id_is_idempotent_after_restart(tmp_path):
    _, path = make_store(tmp_path, snapshot_every=2, reducer=jobs_reducer)
    effects = []
    store = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer,
                             effect_handler=lambda e, data: effects.append(e.action_id),
                             clock=lambda: D)
    first = store.execute('P', 'a1', 0, 'job_scheduled', {'job_id': 'J1'})
    retry = store.execute('P', 'a1', 0, 'job_scheduled', {'job_id': 'J1'})
    assert retry.duplicate and retry.sequence == 1 and retry.version == 1
    assert store.version('P') == 1 and effects == ['a1']

    restarted = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer,
                                 effect_handler=lambda e, data: effects.append(e.action_id),
                                 clock=lambda: D)
    after_restart = restarted.execute('P', 'a1', 1, 'job_scheduled', {'job_id': 'J1'})
    assert after_restart.duplicate and after_restart.version == 1
    # recovery replay and the retried command never re-ran the business action
    assert effects == ['a1']
    restarted.close()


def test_repeated_recovery_does_not_reapply_actions(tmp_path):
    _, path = make_store(tmp_path, snapshot_every=2, reducer=jobs_reducer)
    effects = []
    store = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer,
                             effect_handler=lambda e, data: effects.append(e.action_id),
                             clock=lambda: D)
    for i in range(1, 5):
        store.execute('P', f'a{i}', i - 1, 'job_scheduled', {'job_id': f'J{i}'})
    assert effects == ['a1', 'a2', 'a3', 'a4']

    recovered = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer,
                                 effect_handler=lambda e, data: effects.append(e.action_id),
                                 clock=lambda: D)
    recovered.recover()
    recovered.recover()
    assert effects == ['a1', 'a2', 'a3', 'a4']
    state = recovered.state('P')
    assert state.version == 4 and set(state.data['jobs']) == {'J1', 'J2', 'J3', 'J4'}
    recovered.close()


def test_concurrent_writers_conflict_then_succeed_on_retry(tmp_path):
    _, path = make_store(tmp_path, reducer=jobs_reducer)
    store = DurablePlanStore(path, snapshot_every=100, reducer=jobs_reducer, clock=lambda: D)
    store.execute('P', 'seed', 0, 'plan_created', {})
    errors = []

    def worker(worker_id):
        for i in range(20):
            action = f'w{worker_id}-{i}'
            for attempt in range(20):
                try:
                    store.execute('P', action, store.version('P'),
                                  'job_scheduled', {'job_id': f'J{worker_id}-{i}'})
                    break
                except ConflictError:
                    continue
            else:
                errors.append(action)

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert store.version('P') == 161
    sequences = [e.sequence for e in store.read_events('P')]
    assert sequences == list(range(1, 162))
    # every business action landed exactly once
    state = store.state('P')
    assert len(state.applied_actions) == 161
    assert len(state.data['jobs']) == 160
    store.close()


def test_snapshot_ahead_of_truncated_event_log_is_reported(tmp_path):
    _, path = make_store(tmp_path, snapshot_every=2, reducer=jobs_reducer)
    store = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer, clock=lambda: D)
    for i in range(1, 5):
        store.execute('P', f'a{i}', i - 1, 'job_scheduled', {'job_id': f'J{i}'})
    # external event log loss: verified snapshots reach seq 4, log rewritten to seq 1-2
    events_path = path / 'P' / 'events.jsonl'
    lines = events_path.read_text().splitlines()
    events_path.write_text('\n'.join(lines[:2]) + '\n')

    restarted = DurablePlanStore(path, snapshot_every=2, reducer=jobs_reducer, clock=lambda: D)
    recovery = [p for p in restarted.last_recovery.plans if p.plan_id == 'P'][0]
    assert recovery.snapshot_ahead
    assert restarted.state('P').version == 4
    # reads are safe on the verified snapshot, but appending into the gap fails closed
    with raises(ConflictError):
        restarted.execute('P', 'a5', 4, 'job_scheduled', {'job_id': 'J5'})
    # retries of already applied actions remain idempotent despite the gap
    assert restarted.execute('P', 'a1', 4, 'job_scheduled', {'job_id': 'J1'}).duplicate
    restarted.close()


def test_plan_id_cannot_escape_storage_directory(tmp_path):
    store, _ = make_store(tmp_path)
    for bad in ('../escape', '', 'a/b', 'x' * 200):
        with raises(ValidationError):
            store.execute(bad, 'a1', 0, 'plan_created', {})
    store.close()


def test_default_reducer_deep_merges_payloads(tmp_path):
    store, _ = make_store(tmp_path)
    store.execute('P', 'a1', 0, 'created', {'spec': {'vessels': {'V1': {'berth': 'B1'}}}})
    store.execute('P', 'a2', 1, 'updated', {'spec': {'vessels': {'V2': {'berth': 'B2'}}}})
    state = store.state('P')
    assert state.data['spec']['vessels'] == {'V1': {'berth': 'B1'}, 'V2': {'berth': 'B2'}}
    store.close()


def test_event_records_carry_checksums_and_iso_time(tmp_path):
    _, path = make_store(tmp_path)
    store = DurablePlanStore(path, clock=lambda: D)
    store.execute('P', 'a1', 0, 'created', {'when': D + timedelta(hours=2)})
    row = json.loads((path / 'P' / 'events.jsonl').read_text().splitlines()[0])
    assert len(row['checksum']) == 64
    assert row['occurred_at'] == '2026-01-01T00:00:00Z'
    event = store.read_events('P')[0]
    assert isinstance(event, PlanEvent) and event.occurred_at == D
    store.close()


def test_multiple_plans_recover_independently(tmp_path):
    store, path = make_store(tmp_path, snapshot_every=1, reducer=jobs_reducer)
    store.execute('P1', 'a1', 0, 'plan_created', {})
    store.execute('P2', 'b1', 0, 'plan_created', {})
    store.execute('P1', 'a2', 1, 'job_scheduled', {'job_id': 'J1'})
    restarted = DurablePlanStore(path, snapshot_every=1, reducer=jobs_reducer, clock=lambda: D)
    assert set(restarted.plan_ids()) == {'P1', 'P2'}
    assert restarted.version('P1') == 2 and restarted.version('P2') == 1
    assert restarted.state('P1').applied_actions == {'a1': 1, 'a2': 2}
    assert restarted.state('P2').applied_actions == {'b1': 1}
    restarted.close()
    store.close()
