# TerminalWeave

TerminalWeave is a dependency-light Python engine for modelling day-to-day container-terminal operations. It covers marine access, berth use, quay cranes, yard placement, truck-gate appointments, customs holds, reefer monitoring, hazardous-goods segregation, equipment maintenance, rail blocks, demurrage billing and transshipment connections.

The package is intentionally usable without a database or web framework. Domain services expose ordinary Python APIs, while the persistence layer supplies an append-only event store and snapshots for embedding in larger systems.

### Durable plan storage

`terminalweave.persistence.plan_store.DurablePlanStore` makes plans restartable.
Each plan id owns an ordered, checksummed event stream on disk (`events.jsonl`)
plus atomic snapshots written every `snapshot_every` events, with the previous
snapshot kept as a fallback:

- writes are optimistic — commands pass `expected_version`, so concurrent
  writers (threads or processes; an `flock` guard plus write-time resync protect
  the log) hit `ConflictError` on stale versions instead of clobbering;
- commands also carry an `action_id`; retries are idempotent — a repeated
  `action_id` returns the recorded outcome without appending another event;
- recovery loads the newest valid snapshot and folds every later event; a
  corrupt or checksum-tampered snapshot is discarded in favour of the previous
  generation or a full event replay, and a torn event-log tail is truncated;
- folding uses the supplied pure reducer. Side effects (the optional
  `effect_handler`) run only for genuinely new commands, never during replay,
  so recovering twice never re-executes completed business actions.

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
