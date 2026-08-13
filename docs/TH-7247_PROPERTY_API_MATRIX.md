# TH-7247 property, filter, and list API matrix

This is the release inventory for the current-table optimization. It covers
the APIs whose behavior changes in this PR, their active frontend consumers,
and the select-only qualification matrix. No property catalog, materialized
view, Kafka consumer, span-table rewrite, or ingestion-path change is included.

## API inventory

| API | Changed contract/behavior | Pagination and search | Active frontend consumers |
| --- | --- | --- | --- |
| `GET /api/traces/span-attribute-keys/` | Bounded latest-state key discovery for one project or an authorized workspace. Workspace reads traverse at most 64 projects per physical request and preserve all observed key/type lanes across batches. | Signed cursor; `page_size=1..50`; exact-key `q`; `discovery_mode=filter|eval_mapping`. Partial substring discovery is local over explicitly loaded retained pages. One user action makes one physical request. | LLM Tracing and Voice Basic/Query property pickers; Journey Attributes; eval mapping/test mode; Run Insights trace/span tabs; Widget Editor Trace Attributes; alert filters; Custom Columns; Sessions/Users/User Trace; eval-task create/edit drawers. |
| `GET /tracer/dashboard/filter_values/` | One request-owned four-second wall covers authorization, PostgreSQL metadata, ClickHouse reads, and label hydration. Supports custom, system, project, session, eval, annotation, and annotator values without materializing a workspace-wide vocabulary. Workspace/large explicit scopes advance in authorized 64-project batches. JSON arrays resume within a cell, including arrays with more than 500 members. | Signed cursor; `page_size`; `cursor`; server `search`; `attribute_type`. Each Load more/Retry makes one request. Loaded values survive a bounded fresh-chain Retry. Configured eval/annotation choices preserve typed JSON including `false` and `0`. | LLM Tracing and Voice Basic/Query value pickers; ComplexFilter autocomplete; Widget Editor values; saved filter labels; TaskFilterBar; annotation add-items dialogs. |
| `GET /tracer/dashboard/metrics/` | Callers that do not need custom attributes can set `exclude_custom_attributes=true`, avoiding the legacy capped ClickHouse attribute-catalog scan and workspace project materialization. Widget custom attributes now come from the signed key cursor. | Finite catalog pagination for system/eval/annotation/dataset rows; custom attributes use `/span-attribute-keys/`. | Widget Editor, TraceFilterPanel catalog, tracing graphs, annotation rule dialog. |
| `GET /tracer/trace/list_traces/` | Bounded cursor selector and relational filter compilation for custom attributes, eval results, and annotation completeness. Metadata needed by classifiers is frozen once per operation instead of re-read per batch. | Signed row cursor; explicit p1/p2/pN; incomplete proof returns sanitized `503`; numbered deep pages may return `422` when a safe cursor proof is impossible. | LLM Tracing TraceGrid; eval-task live/test preview; annotation add-items; Run Insights traces. |
| `GET /tracer/trace/list_traces_of_session/` | Same bounded trace filtering and eval/annotation semantics for session-scoped traces. | Signed row cursor/read-more; no silent sampled success unless explicitly requested. | Session trace views and bounded trace export path. |
| `GET /tracer/trace/list_voice_calls/` | Voice now uses the same bounded property/eval/annotation semantics as traces, including all-configured-label annotation completeness and frozen eval metadata. | Signed row cursor; explicit read-more; truthful sampled/degraded metadata. | Voice/Agents grid; eval-task preview; annotation add-items. |
| `GET /tracer/observation-span/list_spans/` | Bounded span filters with authoritative eval/annotation metadata and explicit known-empty annotation semantics. | Signed row cursor; explicit p1/p2/pN; sanitized `503` on incomplete proof. | SpanGrid, task/eval previews, annotation add-items, Run Insights spans. |
| `GET /tracer/observation-span/list_spans_observe/` | Observe span list uses the same bounded relational filter semantics. | Signed row cursor/read-more. | Observe/LLM tracing span surfaces and bounded export. |
| `GET /tracer/trace-session/list_sessions/` | Bounded session filters, project-scoped session label hydration, and memoized eval metadata across classifier batches. | Signed row cursor/read-more. | Sessions grid and session-based filtering. |
| `GET /tracer/trace/get_trace_export_data/` | Voice detection routes through the bounded voice list and preserves the legacy CSV schema with a truthful truncation marker. | One bounded export page; this is not an exhaustive all-row export. | Export/API consumer; no current direct frontend caller was found. |
| `GET /tracer/eval-task/list_eval_tasks/` | COUNT and OFFSET/LIMIT happen before hydration for exactly translatable numeric/datetime filters and sorts. Only page eval configs are prefetched. | Numbered page; exact total. Arbitrary/text result filters deliberately retain the legacy compatibility fallback. | Eval Tasks list. |
| `GET /tracer/eval-task/list_eval_tasks_with_project_name/` | Same bounded ORM fast path with project name in the response. | Numbered page; exact total. | Organization/workspace eval-task list. |
| `GET /tracer/eval-task/get_usage/` | Bounded newest-row usage aggregation with truthful sampling metadata and indexed task/time lookup. | Periods through 365 days/custom up to 366 days. | Eval Task Usage tab. |
| Eval-task historical and continuous row resolution | Span, trace, session, and voice candidates resolve configured eval IDs, eval output metadata, and annotation label IDs once per operation. Cursor reconciliation advances only after a complete classifier proof. | Finite candidate/query budgets; retryable sanitized failure, never partial cursor publication. | Eval-task create/update/unpause execution paths and live previews. |

## Time and population qualification matrix

Every list/filter cell is exercised for these retained windows:

| UI period | Qualification interval |
| --- | --- |
| Recent | 1 hour |
| Today | 24 hours |
| 7D | 7 days |
| 30D | 30 days |
| 3M | 90 days |
| 6M | 180 days |
| 12M | 365 days |

Population profiles:

- **Dense:** Whatfix trace data; page one, page two, page four/former late-page
  failure boundary, and at least ten distinct key cursor advances.
- **Sparse:** Colektia/Colly trace data; empty advancing checkpoints, rare
  keys/values, truthful exhaustion, and repeatability.
- **Voice:** Mudflap voice data; property parity plus eval/annotation positive
  and negative membership.

Filter combinations used for trace, span, session, and voice lists:

| ID | Filter cell |
| --- | --- |
| F0 | Date/project only |
| F1 | System field |
| F2 | Sparse custom property/value |
| F3 | Dense custom property/value |
| F4 | System plus custom property |
| F5 | `has_eval=true|false` and exact eval value |
| F6 | `has_annotation=true|false`; positive requires every configured label |
| F7 | Custom property plus eval plus annotation conjunction |

For each applicable cell, qualification checks p1, p2, and continued pN or a
truthful complete terminal. Cursor advancement, no duplicate/gap publication,
project/workspace fencing, exact positive/negative membership, and identical
repeat requests are asserted.

## Latency and read-more contract

- Property key and value picker **physical requests** must complete in under
  five seconds. The server read wall is four seconds; the client transport
  timeout is 4.8 seconds.
- Trace/span/session/voice list and eval/annotation filter requests must
  complete in under 9.8 seconds (the product requirement is under 10 seconds).
- Load more and Retry are explicit, single-flight, one-request actions. Window
  focus, remount, reconnect, inertial scrolling, and React Query refetch do not
  replay cached infinite pages.
- A malformed/repeated cursor fails safely, preserves already loaded rows, and
  offers one bounded fresh-chain retry. It never masquerades as exhaustion.
- Request/query/page budgets are work bounds, not product history or result
  caps. When more retained data exists, the response carries a signed
  continuation.

## Explicit release boundaries

- Key `q` is an exact-key accelerator. Full substring discovery requires
  explicit retained-catalog pagination; this PR does not add a substring
  index.
- Cursor-backed categorical annotation values exhaust configured choices and
  truthfully report stored Score history as sampled. Exhaustive stored-only
  Score history needs the later composite-index/catalog PR.
- Dataset/simulation legacy value endpoints are outside this tracing/voice
  release matrix.
- The later PostHog-style stacked PR will add independent ingestion-fed lookup
  storage. It will not modify the existing spans table and will be designed to
  avoid the prior materialized-view ingestion OOM failure mode.
