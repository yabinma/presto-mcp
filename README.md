# presto-mcp

A read-only [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for
Presto and Trino engines.

It exposes engines, catalogs, schemas, tables, and queries to AI agents for
**statistics, auditing, and performance analysis** — without granting the ability to
run arbitrary SQL.

> **Status:** both deployment shapes are implemented — **local** (stdio + static credentials)
> and **enterprise** (streamable-HTTP + passthrough credentials, distroless image + k8s
> manifest).

## Why

Agents that help operate a data platform need to *read* its state — what catalogs exist,
how a table is shaped, why a query was slow — but giving an agent a raw SQL connection is
a large, hard-to-audit attack surface. `presto-mcp` exposes only a curated set of
**read-only** tools, normalizes results into compact structures so they don't blow up the
agent's context, and never holds an identity of its own in the enterprise deployment.

## Features

- **Read-only by construction** — a fixed set of curated tools, no write/DDL SQL. The one
  SQL entry point, `run_query`, runs allow-listed read-only statements only (no
  INSERT/UPDATE/DELETE/CREATE/DROP/...), with capped result sets.
- **Multi-engine** — connect to several Presto/Trino engines at once; tools
  address an engine by a stable logical `id`.
- **Two deployment shapes from one codebase**:
  - **Local** — embedded in a developer's agent tooling over **stdio**, with credentials
    configured statically per engine.
  - **Enterprise (co-located)** — deployed inside enterprise infrastructure over
    **streamable-HTTP**, with the user's credential passed through from the agent and
    Presto performing authn/authz. The server holds no identity of its own.
- **Credentials as references** — config stores references (`env://`, `file://`); the
  actual secret is resolved at connection time by a credential provider.
- **Query auditing** — list recent queries (filter by state, user, and time range) and get
  **normalized** per-query performance detail (`QueryDetail`: summary / stages / operators /
  plan) instead of raw `QueryInfo` JSON, read from the live coordinator.

## Architecture

```
Agent / MCP client
   │  (stdio or streamable-HTTP; credentials ride the connection-level auth header)
   ▼
┌─────────────── Presto MCP server ───────────────────┐
│  Transport (stdio | streamable-http)      ← edge      │
│  Tool handlers (read-only, stateless, take engine)    │
│  Engine registry (config-driven)                      │
│  Credential provider (static | passthrough) ← edge    │
│  Presto client (Presto / Trino dialects)              │
│  Result normalizer (incl. QueryDetail)                │
└───────────────────────────────────────────────────────┘
   │
   ▼
Presto / Trino engines
(coordinator + catalogs)
```

**Shared core + swappable edges.** Tools, registry, Presto client, result normalization,
and the credential-provider interface are shared across both deployment shapes. Only the
**transport** and the **credential strategy** switch per shape.
`deployment_mode` (`local | enterprise`) selects the defaults — it does not fork the core
logic — and per-engine overrides are allowed.

The server does **not** manage networking: every connected Presto must already be
network-reachable from the server, which the deployment guarantees.

## Tools

All tools are read-only and stateless, and (except `list_engines`) take an `engine` argument.

| Tool | Purpose | Source |
|---|---|---|
| `list_engines()` | List available engines | registry |
| `list_catalogs(engine)` | Catalog list | live |
| `list_schemas(engine, catalog)` | Schema list | live |
| `list_tables(engine, catalog, schema)` | Table list | live |
| `describe_table(engine, catalog, schema, table)` | Columns / types / partitions | live |
| `get_table_stats(engine, ...)` | Row count / size (best-effort) | live |
| `get_cluster_info(engine)` | Version / nodes / memory pools | live |
| `list_queries(engine, {state, user, time_range})` | Auditing | coordinator |
| `get_query(engine, query_id, raw?)` | Performance detail (see below) | coordinator |
| `run_query(engine, sql, catalog?, schema?, max_rows?, timeout_seconds?)` | Run one read-only query (see below) | live |

`list_queries` and `get_query` read the live coordinator and report it in a `source` field.
The coordinator only remembers recent queries, so an empty result can mean "no such query"
or "older than the coordinator's retention".

### `get_query` → `QueryDetail`

Rather than passing raw `QueryInfo` JSON through, `get_query` returns a compact model:

- **`summary`** (always present) — state, user, start/end time, wall/CPU time, peak memory,
  scanned/output rows & bytes, error info.
- **`stages[]`** (when available) — per-stage CPU / wall / blocked time, input/output size.
- **`operators[]`** (when available) — operator-level step costs (the "cost of each step").
- **`plan`** (when available) — a compact representation of the plan-node tree.
- **`source`** — the answering source; **`available_sections`** — which sections were populated.
- The raw fragment is returned only when `raw=true`.

The coordinator usually has stages/operators/plan complete, but only while the query is still
in memory; once it ages out of memory, only `summary` may remain.

### `run_query` — read-only query execution

`run_query` is the one tool that runs caller-supplied SQL, and it runs **read-only statements
only**. The statement is validated against a leading-keyword allowlist —
`SELECT` / `WITH` / `SHOW` / `DESCRIBE` / `EXPLAIN` / `VALUES` / `TABLE`, a single statement,
with `EXPLAIN` re-checking its inner statement — before it is sent to the engine verbatim.
Writes, DDL, `CALL`, `USE`, `SET`, and transaction control are rejected. This guard is layered
on top of the engine's own access control (there is no Postgres-style read-only transaction in
Trino/Presto), so a least-privilege engine account is still recommended.

- Returns `columns` (name + engine type), `rows`, `row_count`, and `truncated`.
- Optional `catalog` / `schema` set the session defaults for resolving unqualified names.
- `max_rows` caps the result (default **1000**, max **10000**). When the engine has more rows
  than the cap, the surplus is dropped, `truncated=true`, paging stops, and the still-running
  statement is best-effort cancelled on the coordinator.
- `timeout_seconds` bounds the whole query (default **60**, max **300**).

The tool is always available; there is no per-engine flag to enable it.

## Configuration

Configuration is YAML. Credentials are always references, resolved at connection time.

```yaml
deployment_mode: local        # local | enterprise

server:
  transport: stdio            # stdio (local) | http (enterprise)
  # http: { host: 0.0.0.0, port: 8080 }

engines:
  - id: dev-presto            # stable logical name; tools reference the engine by this
    endpoint: https://presto-dev.internal:8443
    dialect: presto           # presto | trino
    auth:
      mode: static            # static (local) | passthrough (enterprise)
      credential_ref: env://DEV_PRESTO_TOKEN

  - id: prod-trino
    endpoint: https://trino-prod.internal:8443
    dialect: trino
    auth:
      mode: passthrough       # use the user's credential passed through by the agent
```

### Authentication

`auth.scheme` selects how the resolved credential is sent to the engine (all secrets
stay references — `env://`, `file://`):

| Scheme | Engine auth | Sends | Config |
|---|---|---|---|
| *(none)* | unsecured | only `X-{Presto,Trino}-User` | `user:` only |
| `bearer` (default) | JWT / OAuth2 | `Authorization: Bearer <token>` | `credential_ref:` → the token |
| `basic` | username / password (LDAP, file) | `Authorization: Basic <base64(user:password)>` | `user:` + `password_ref:`; **https required** |

For `bearer`, the token is whatever your identity platform issues (a JWT or OAuth2
access token) — a username/password is **not** a bearer token. For `basic`, the password
is referenced (never inline) and the endpoint must be `https` so credentials are never sent
over plaintext. See [config.example.yaml](config.example.yaml) for one engine of each kind.

For engines with a self-signed certificate (typical in dev/test), set
`tls_insecure_skip_verify: true` on the engine to skip certificate verification. **Do not
use this in production** — provide a properly trusted certificate instead.

## Getting started

### Build

```bash
go build ./...
go test ./...
```

### Run (local / stdio)

```bash
presto-mcp --config config.yaml
```

Then point your MCP client at the binary over stdio. Example client entry:

```json
{
  "mcpServers": {
    "presto": {
      "command": "presto-mcp",
      "args": ["--config", "/path/to/config.yaml"]
    }
  }
}
```

### Run (enterprise / streamable-HTTP)

Set `deployment_mode: enterprise` (defaults to `http` transport + `passthrough`
credentials), deploy co-located with Presto, and point the agent at the HTTP endpoint.
The user's credential rides the request's `Authorization` header and is forwarded to Presto
as-is; the server holds no identity of its own. See
[`config.enterprise.example.yaml`](config.enterprise.example.yaml).

```bash
make docker                          # build the distroless image (presto-mcp:dev)
kubectl apply -f deploy/kubernetes.yaml   # ConfigMap + Deployment + Service (probes /healthz)
```

The MCP HTTP edge supports TLS, an `Origin` allowlist (DNS-rebinding protection), a
`/healthz` probe, and **optional** bearer verification (`edge_auth.scheme: jwt_rs256`) — the
default is opaque passthrough (forward the token unverified; the engine is the authority).

## Tech stack

- **Language:** Go 1.26
- **MCP:** the official [`github.com/modelcontextprotocol/go-sdk`](https://github.com/modelcontextprotocol/go-sdk)
  (one codebase covers both stdio and streamable-HTTP)
- **HTTP:** standard-library `net/http` (Presto REST API)
- **Config:** YAML (`gopkg.in/yaml.v3`) + struct validation
- **Testing:** `testing` + `testify`; interfaces + fakes for mocking; Presto REST stubbed
  with `httptest.Server`; functional tests via `testcontainers-go` (real Trino + Presto)

## Testing

- **Unit tests** run with the race detector and gate the build at **≥ 80% coverage**, with
  no dependency on any third-party service — Presto REST is stubbed with `httptest.Server`
  and all dependencies are injected as fakes through interfaces. Run `make test` / `make cover`.
- **Benchmarks** cover the major code paths (parsing, normalization, request handling); the
  parallel ones run under `-race` to surface contention. Run `make bench`.
- **Functional tests** drive every tool end-to-end through an in-memory MCP client against
  **real Trino and Presto** containers in local deployment mode. Dedicated cases also run
  **TLS-enabled engines** to exercise the secured auth schemes over https against both
  dialects — `basic` (file password authenticator) and `bearer`/JWT (RS256, engine-verified)
  — each with a negative check that a wrong credential is rejected. An **enterprise** suite
  additionally runs the built **container image** alongside the engines on a shared Docker
  network and drives it over HTTP, proving passthrough end-to-end (caller token forwarded and
  accepted; untrusted token engine-rejected; no-credential refused) for both dialects. They are
  behind the `functional` build tag (need Docker), so `go test ./...` skips them and they do
  not count toward the coverage gate. Run `make func-test`.

```bash
make test        # unit tests (-race)
make cover       # unit tests + coverage gate (>= 80%)
make bench       # benchmarks
make func-test   # functional suite against Trino + Presto (Docker required)
make docker      # build the distroless container image
```

## License

[Apache 2.0](LICENSE).
