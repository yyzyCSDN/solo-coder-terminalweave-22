# TerminalWeave

TerminalWeave is a dependency-light Python engine for modelling day-to-day container-terminal operations. It covers marine access, berth use, quay cranes, yard placement, truck-gate appointments, customs holds, reefer monitoring, hazardous-goods segregation, equipment maintenance, rail blocks, demurrage billing and transshipment connections.

The package is intentionally usable without a database or web framework. Domain services expose ordinary Python APIs, while the persistence layer supplies an append-only event store and snapshots for embedding in larger systems.

## Quick start

```bash
python -m pip install -e ".[test]"
python -m terminalweave.cli demo
python -m pytest -q
```

Run the small HTTP adapter with `terminalweave serve`. It exposes `GET /health`,
`POST /tide/clearance`, and `POST /billing/demurrage`. The other packages are
library components intended to be composed by an embedding application.

## Design principles

- domain calculations are deterministic for identical inputs;
- time values are timezone-aware UTC datetimes;
- operational state transitions are explicit and validated;
- safety constraints fail closed when required information is absent;
- money uses `Decimal`;
- core domain modules depend only on the Python standard library.

## Main packages

`marine`, `cranes`, `yard`, `gate`, `customs`, `reefer`, `hazmat`, `equipment`, `rail`, `billing`, `transshipment`, `planning`, `persistence`, `reporting`, and `api`.

## Restartable plan storage

`persistence.RestartablePlanStore` gives plan aggregates an event-sourced,
snapshot-backed lifetime that survives process restarts:

```python
from terminalweave.persistence import RestartablePlanStore, ActionContext

store = RestartablePlanStore("./data/plans", snapshot_interval=25)

def on_item(ctx: ActionContext):
    # side-effecting business actions must be idempotent on ctx.key
    external_system.schedule(ctx.payload["item_id"], idempotency_key=ctx.key)

store.register_action("plan.item_added", on_item)

result = store.create_plan("berth-7", "Night rotation")
store.add_item("berth-7", result.version, "move-1", {"crane": "QC3"},
               command_id="client-command-123")

# after a restart: snapshot validated, missing events replayed, interrupted
# actions redriven — completed actions never run twice
for report in store.recover_all():
    print(report.plan_id, report.recovered_sequence, report.snapshot_used)
```

Guarantees:

- every plan change is an ordered, checksummed event appended to a per-plan
  durable log (fsynced); an append passes the caller's expected sequence and a
  stale version raises `ConflictError`, across threads *and* processes;
- a `command_id` makes retries idempotent (a repeated command returns the
  original event instead of appending a duplicate);
- state snapshots are written periodically and atomically with a SHA-256
  checksum; on recovery a missing, corrupt/truncated, or future-dated snapshot
  is discarded or quarantined, state is rebuilt by replaying the event log,
  and a fresh snapshot is taken;
- business actions run at-least-once and are tracked in a durable action
  ledger keyed by `plan_id:sequence`; repeated recoveries never re-execute
  completed actions, including actions already folded into a snapshot.

