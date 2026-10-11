# linexus-logger
Immutable Audit Trail for the Linexus ecosystem — Segmented log, Demiurge append-only ledger, decay sweeper. The black box of a civilization that honors the breath.

## The operational audit log

`src/audit.rs` / `src/api.rs` are the RMM half: Nexus ingests agents' events
and tails them back for the Hub.

| Route | |
| --- | --- |
| `POST /logs` | One entry or an array; an array is stored atomically (all or nothing) |
| `GET /logs` | Most-recent-first. Filters: `agent_id`, `task_id`, `level`, `source`, `since` / `until` (RFC 3339, inclusive; a malformed value is `400`), `before` (alias `cursor`: the id of the oldest record of the previous page, to page further back), `limit` (default 100, max 1000) |
| `GET /healthz` | Liveness |
| `GET /metrics` | Prometheus counters: `logger_http_requests_total`, `logger_logs_ingested_total`, `logger_logs_pruned_total` (unauthenticated, like `/healthz`) |

| Variable | Default | Meaning |
| --- | --- | --- |
| `LOGGER_BIND` | `0.0.0.0:5151` | Listen address |
| `LOGGER_DATABASE_URL` | `sqlite://linexus-logger.sqlite` | SQLite file |
| `LOGGER_SERVICE_TOKEN` | unset | Bearer token Nexus presents. Unset means open (development only) |
| `LOGGER_RETENTION_DAYS` | `90` | Events older than this are deleted at start and every hour; `0` keeps everything |
| `LINEXUS_ENV` | unset | `production` marks a real deployment: the service **refuses to start** without `LOGGER_SERVICE_TOKEN` instead of warning |
