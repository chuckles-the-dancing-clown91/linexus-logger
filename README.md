# linexus-logger

Immutable Audit Trail for the Linexus ecosystem — the operational log service
behind Nexus, plus the Demiurge append-only ledger and decay sweeper. The
black box of a civilization that honors the breath.

## What it does

Agents' journal lines and task audit events are shipped here (always via
Nexus — nothing talks to the Logger directly except Nexus), persisted to
append-only JSONL segments on disk, and served back for the log viewers in
Daedalus IT. A segment is never rewritten: append is the only mutation, and
retention deletes whole expired segments rather than editing them — an audit
trail you can edit is a notebook, not an audit trail.

Queries run against a bounded in-memory index rebuilt from the segment files
at boot, so a restart loses nothing and the read path never touches disk.
Torn lines from a crash mid-write are skipped on replay, never allowed to
take the remaining history offline.

## HTTP surface

| Route | Purpose |
| --- | --- |
| `GET /healthz` | liveness + counters (indexed entries, active segment) |
| `POST /logs` | ingest one entry or a batch: `{message, level?, source?, agent_id?, task_id?, timestamp?, metadata?}` — omitted timestamps are stamped at ingest; a supplied one wins (the shipper observed the event) |
| `GET /logs?agent_id=&task_id=&level=&limit=` | most-recent-first query, `limit` clamped to 1–1000 |

Auth is the shared bearer service token (`LINEXUS_SERVICE_TOKEN`), the same
credential Nexus presents to the Orchestrator. Unset runs the service open
for local development; set, every logs route requires it — an open write
endpoint on an audit trail would let anyone manufacture history.

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `LOGGER_BIND` | `0.0.0.0:5151` | listen address (matches Nexus's `LINEXUS_LOGGER_URL` default) |
| `LOGGER_DATA_DIR` | `./logger-data` | segment directory |
| `LOGGER_SEGMENT_ENTRIES` | `10000` | entries per segment before rotation |
| `LOGGER_INDEX_ENTRIES` | `50000` | in-memory query window |
| `LOGGER_RETENTION_DAYS` | `30` | drop whole segments older than this (hourly sweep; the active segment is never touched) |
| `LINEXUS_SERVICE_TOKEN` | *(unset = open)* | shared bearer token |

## Correlation

`task_id` is the key that follows an action end to end: the same id appears
in the Orchestrator's plan, the agent's journal lines, this log, and the
TaskRun history in Daedalus IT. `agent_id` scopes a machine's journal for
the Hub's log viewer (`GET /api/v1/agents/{id}/logs` on Nexus relays from
here).

## Run

```bash
cargo run                       # dev, open auth, ./logger-data
cargo test                      # segment rotation, replay, filters, auth
```
