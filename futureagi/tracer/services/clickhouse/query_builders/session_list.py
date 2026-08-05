"""
Session List Query Builder for ClickHouse.

Replaces the ``list_sessions()`` method in ``tracer.views.trace_session``
with a ClickHouse query that groups the denormalized ``spans`` table by
``trace_session_id``.

Because the ``spans`` table denormalizes trace context (including session
ID) into every span row, we can compute per-session aggregates in a single
``GROUP BY`` without JOINs.
"""

from datetime import datetime
from typing import Any

from tracer.services.clickhouse.query_builders.base import NIL_UUID, BaseQueryBuilder
from tracer.services.clickhouse.query_builders.filters import ClickHouseFilterBuilder
from tracer.services.clickhouse.query_builders.latest_filter_predicates import (
    partition_span_filter_plans,
)
from tracer.services.clickhouse.query_builders.session_filters import (
    SESSION_ID_FILTER_COLS,
    build_session_id_filter_clause,
)
from tracer.services.clickhouse.v2.id_remap_sql import (
    remap_left_join,
    resolved_id_expr,
    survivor_map_subquery,
)


class SessionListQueryBuilder(BaseQueryBuilder):
    """Build queries for the paginated session list view.

    Computes per-session aggregates:
    - ``min(start_time)`` -- session start
    - ``max(end_time)`` -- session end
    - ``sum(cost)`` -- total cost
    - ``sum(total_tokens)`` -- total tokens
    - ``uniq(trace_id)`` -- number of traces (HyperLogLog, ~2% error)
    - ``argMin(input, start_time)`` -- first user message
    - ``argMax(input, start_time)`` -- last user message

    Args:
        project_id: Project UUID string.
        page_number: Zero-based page index.
        page_size: Number of sessions per page.
        filters: Frontend filter list.
        sort_params: Frontend sort specification list.
        user_id: Optional end-user ID to restrict sessions.
    """

    TABLE = "spans"
    # The v2 subclass swaps this for the CH25-aware compiler.  Keeping the
    # compiler behind a class attribute also lets the bounded session selector
    # compile candidate-scoped residual predicates without leaking v1 column
    # names into a v2-only deployment.
    _FILTER_BUILDER_CLS = ClickHouseFilterBuilder

    # Mapping from frontend sort column names to ClickHouse expressions
    SORT_FIELD_MAP: dict[str, str] = {
        "created_at": "session_start",
        "start_time": "session_start",
        "end_time": "session_end",
        "duration": "duration",
        "total_cost": "total_cost",
        "total_tokens": "total_tokens",
        "traces_count": "traces_count",
        "total_traces_count": "traces_count",
    }

    # Session-level filter columns that map to computed aggregates
    SESSION_FILTER_MAP: dict[str, str] = {
        "duration": "duration",
        "total_cost": "total_cost",
        "total_tokens": "total_tokens",
        "traces_count": "traces_count",
        "total_traces_count": "traces_count",
    }

    MESSAGE_FILTER_MAP: dict[str, str] = {
        "first_message": "first_message",
        "last_message": "last_message",
    }

    # Aggregate projections shared by build() and build_id_query() so a HAVING on
    # any of these aliases resolves in both (build_id_query returns only session_id
    # but still applies the same HAVING).
    _AGGREGATE_SELECT = (
        "min(start_time) AS session_start, "
        "max(end_time) AS session_end, "
        "dateDiff('second', min(start_time), max(end_time)) AS duration, "
        "sum(cost) AS total_cost, "
        "sum(total_tokens) AS total_tokens, "
        "uniq(trace_id) AS traces_count"
    )

    _CANDIDATE_SORT_FIELDS: dict[str, str] = {
        **SORT_FIELD_MAP,
        "session": "session_id",
        "session_id": "session_id",
        "trace_session_id": "session_id",
        "first_message": "first_message",
        "last_message": "last_message",
    }

    def __init__(
        self,
        project_id: str | None = None,
        project_ids: list[str] | None = None,
        page_number: int = 0,
        page_size: int = 50,
        filters: list[dict] | None = None,
        sort_params: list[dict] | None = None,
        user_id: str | None = None,
        bounded_internal_scan: bool = False,
        bounded_sampling_salt: str | None = None,
        bounded_sampling_rate: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(project_id=project_id, project_ids=project_ids, **kwargs)
        self.page_number = page_number
        self.page_size = page_size
        self.filters = filters or []
        self.sort_params = sort_params or []
        self.user_id = user_id
        self._bounded_internal_scan = bool(bounded_internal_scan)
        if (bounded_sampling_salt is None) != (bounded_sampling_rate is None):
            raise ValueError(
                "bounded_sampling_salt and bounded_sampling_rate must be paired"
            )
        if bounded_sampling_rate is not None and not (
            0 <= float(bounded_sampling_rate) <= 100
        ):
            raise ValueError("bounded_sampling_rate must be between 0 and 100")
        self._bounded_sampling_salt = bounded_sampling_salt
        self._bounded_sampling_rate = bounded_sampling_rate
        self.start_date: datetime | None = None
        self.end_date: datetime | None = None
        # The default range is derived from ``utcnow``. Pin it once so the
        # bounded selector and every seed/classifier query use identical
        # half-open boundaries instead of drifting forward by microseconds.
        self._bounded_request_window = BaseQueryBuilder.parse_time_range(
            self.filters, strict=True
        )

    def parse_time_range(
        self, filters: list[dict]
    ) -> tuple[datetime | None, datetime | None]:
        if filters is self.filters or filters == self.filters:
            return self._bounded_request_window
        return BaseQueryBuilder.parse_time_range(filters, strict=True)

    def _bounded_scalar_span_filters(self) -> list[dict[str, Any]]:
        """Return root-span predicates not handled at session level.

        Session identity, end-user membership, aggregate/message predicates and
        the request window are evaluated by the session CTEs.  Everything else
        (notably customer attributes such as ``final_status``) is compiled as a
        latest-state root-span predicate for a finite candidate-session batch.
        """

        session_columns = {
            "created_at",
            "start_time",
            "end_time",
            *self._SESSION_ID_FILTER_COLS,
            *self._ENDUSER_ID_FILTER_COLS,
            *self.SESSION_FILTER_MAP,
            *self.MESSAGE_FILTER_MAP,
        }
        return [
            item
            for item in self.filters
            if (item.get("column_id") or item.get("columnId")) not in session_columns
        ]

    def _bounded_span_filter_parts(self):
        return partition_span_filter_plans(self._bounded_scalar_span_filters())

    @staticmethod
    def _bounded_root_witness_plan(plans: list[Any] | tuple[Any, ...]):
        """Choose the most selective safe raw-row witness deterministically."""

        candidates = [
            (
                getattr(plan, "raw_witness_rank", None)
                if getattr(plan, "raw_witness_rank", None) is not None
                else 100,
                index,
                plan,
            )
            for index, plan in enumerate(plans)
            if getattr(plan, "raw_witness_predicate", None) is not None
        ]
        return min(candidates, default=(None, None, None))[-1]

    def bounded_filter_degraded_error_code(self) -> str | None:
        """Explain why the finite session bulk selector cannot represent a shape."""

        if self.sort_params:
            # Bulk selection has a fixed newest-session order.  An arbitrary UI
            # sort would require a global aggregate before a finite prefix can
            # be proven.
            return "unsupported_filter_modifiers"
        try:
            _, residual = self._bounded_span_filter_parts()
        except (TypeError, ValueError):
            return "unsupported_filter_shape"
        if residual:
            # Score-label filters are removed by the caller and intersected in
            # PG.  Other relational eval/annotation filters do not yet have a
            # session-keyed finite classifier and must never fall back broad.
            return "unsupported_relational_session_filter"
        return None

    def supports_bounded_filter_scan(self) -> bool:
        active = any(
            (item.get("column_id") or item.get("columnId"))
            not in {"created_at", "start_time"}
            for item in self.filters
        )
        return (
            active or self._bounded_internal_scan
        ) and self.bounded_filter_degraded_error_code() is None

    @staticmethod
    def recommended_filter_seed_batch_size() -> int:
        return 200

    @staticmethod
    def recommended_filter_classify_batch_size() -> int:
        # Attribute argMax replay still touches complete Map state for every
        # physical root in the candidate sessions.  Production's largest
        # tenant exceeded the endpoint deadline at 200; keep seed acquisition
        # broad and split only the exact latest-state replay.
        return 50

    @staticmethod
    def filter_seed_proves_result_order() -> bool:
        """The newest raw root is an upper bound for a session's true start.

        Classification returns ``min(live root start_time)``.  An unseen
        session whose newest raw root is below a proved cutoff cannot move ahead
        of that cutoff after latest-version/tombstone replay.
        """

        return True

    def supports_candidate_first_page(self) -> bool:
        """Return true for the exact root-time ordered fast path.

        The physical-root selector below proves the default/created-at ordering.
        Session-id filters are applied to remap-resolved root IDs before paging.
        Positive end-user filters use a user-ID-keyed membership selector;
        negated/null filters use the same project/time-scoped latest-state
        membership stream without unsafe ID pruning. Session aggregate/message
        predicates and their sorts are computed from the narrow physical-latest
        root stream before paging. Arbitrary span/eval/annotation filters remain
        off the list path; the internal bulk selector can classify scalar span
        filters after it has a finite session-ID batch.
        """

        for item in self.filters:
            column_id = item.get("column_id") or item.get("columnId")
            if column_id in {"created_at", "start_time"}:
                continue
            if column_id in self._SESSION_ID_FILTER_COLS:
                continue
            if column_id in self._ENDUSER_ID_FILTER_COLS:
                config = item.get("filter_config") or item.get("filterConfig") or {}
                operator = config.get("filter_op") or config.get("filterOp")
                if operator in {
                    "equals",
                    "in",
                    "not_equals",
                    "not_in",
                    "is_null",
                    "is_not_null",
                }:
                    continue
            if (
                column_id in self.SESSION_FILTER_MAP
                or column_id in self.MESSAGE_FILTER_MAP
                or column_id == "end_time"
            ):
                continue
            return False
        return all(
            (item.get("column_id") or item.get("columnId"))
            in self._CANDIDATE_SORT_FIELDS
            and str(item.get("direction") or "desc").lower() in {"asc", "desc"}
            for item in self.sort_params
        )

    def _candidate_order_clause(self) -> str:
        if not self.sort_params:
            return "ORDER BY session_start DESC, session_id DESC"
        parts: list[str] = []
        sorted_by_session_id = False
        tie_direction = "DESC"
        for item in self.sort_params:
            column_id = item.get("column_id") or item.get("columnId")
            column = self._CANDIDATE_SORT_FIELDS[column_id]
            direction = str(item.get("direction") or "desc").upper()
            tie_direction = direction
            parts.append(f"{column} {direction}")
            sorted_by_session_id = sorted_by_session_id or column == "session_id"
        if not sorted_by_session_id:
            # A total order is required for stable numbered pages.
            parts.append(f"session_id {tie_direction}")
        return f"ORDER BY {', '.join(parts)}"

    def _candidate_positive_filter_values(
        self, columns: frozenset[str]
    ) -> tuple[str, ...]:
        """Return positive equality/IN IDs used to prune candidate identities."""

        values: list[str] = []
        for item in self.filters:
            column_id = item.get("column_id") or item.get("columnId")
            if column_id not in columns:
                continue
            config = item.get("filter_config") or item.get("filterConfig") or {}
            operator = config.get("filter_op") or config.get("filterOp")
            if operator not in {"equals", "in"}:
                continue
            raw = config.get("filter_value", config.get("filterValue"))
            raw_values = raw if isinstance(raw, list) else [raw]
            values.extend(str(value) for value in raw_values if value)
        return tuple(dict.fromkeys(values))

    def _candidate_survivor_map_sql(
        self,
        params: dict[str, Any],
        session_ids: tuple[str, ...],
    ) -> str:
        """Return an exact survivor map for one finite session-ID set.

        The remap table is ordered only by ``old_id``. Resolve old-ID inputs with
        a primary-key point probe, then perform one authoritative reverse
        ``new_id`` pass for both old and new inputs. The caller materializes this
        relation once as a scalar tuple array, so ClickHouse never repeats that
        reverse pass when the classifier consumes the map in several stages.
        """

        if not session_ids:
            raise ValueError("candidate survivor map requires bounded IDs")
        params["candidate_filter_session_id_array"] = list(session_ids)
        return """
            WITH
            candidate_filter_ids AS (
                SELECT arrayJoin(
                    CAST(%(candidate_filter_session_id_array)s AS Array(UUID))
                ) AS candidate_id
            ),
            candidate_target_new_ids AS (
                SELECT DISTINCT new_id
                FROM trace_session_id_remap FINAL
                PREWHERE old_id IN (
                    SELECT candidate_id FROM candidate_filter_ids
                )
                UNION DISTINCT
                SELECT candidate_id AS new_id
                FROM candidate_filter_ids
            ),
            candidate_remap_groups AS (
                SELECT
                    new_id,
                    argMin(old_id, toString(old_id)) AS survivor_id,
                    arrayDistinct(
                        arrayConcat(groupArray(old_id), [new_id])
                ) AS group_ids
                FROM trace_session_id_remap FINAL
                WHERE new_id IN (
                    SELECT new_id FROM candidate_target_new_ids
                )
                GROUP BY new_id
            )
            SELECT arrayJoin(group_ids) AS any_id, survivor_id
            FROM candidate_remap_groups
        """

    def _candidate_survivor_map_ctes(
        self,
        params: dict[str, Any],
        session_ids: tuple[str, ...],
    ) -> str:
        """Materialize one finite map as a query-wide scalar tuple array.

        ClickHouse table CTEs are macros, not materialized results. The session
        classifier references its map while seeding, expanding and replaying;
        embedding the base relation directly would repeat the dimension/remap
        reads for each reference. A scalar subquery is executed once as a set,
        while the tiny array can be expanded repeatedly without table reads.
        """

        map_sql = self._candidate_survivor_map_sql(params, session_ids)
        return f"""
        (
            SELECT groupArray(tuple(any_id, survivor_id))
            FROM ({map_sql})
        ) AS candidate_session_pairs,
        ts_survivor_map AS (
            SELECT
                tupleElement(pair, 1) AS any_id,
                tupleElement(pair, 2) AS survivor_id
            FROM (
                SELECT arrayJoin(candidate_session_pairs) AS pair
            )
        )
        """

    def _candidate_session_ctes(
        self,
        params: dict[str, Any],
        *,
        candidate_session_ids: tuple[str, ...] = (),
        root_filter_plans: tuple[Any, ...] = (),
        candidate_full_state: bool = False,
    ) -> str:
        """Build the shared exact session candidate CTEs.

        Root identities are selected from the root projection and replayed to
        latest physical state. Structural session predicates bind to the
        remap-resolved session ID. Positive end-user predicates build a narrow
        membership set from only the requested user IDs, replaying those span
        identities before resolving both user and session remaps.
        """

        if candidate_full_state and not candidate_session_ids:
            raise ValueError("full-state session scan requires bounded candidates")

        resolved_session = resolved_id_expr("latest_trace_session_id", "ts_remap")
        resolved_session_clause = build_session_id_filter_clause(
            self.filters,
            params,
            session_col="session_id",
            param_prefix="candidate_sess_",
        )
        aggregate_clause = self._build_having_clauses()
        # `_build_having_clauses` maintains the legacy builder contract by
        # binding into `self.params`; copy those generated values into this
        # candidate statement's independent parameter dict.
        params.update(self.params)

        candidate_columns = {
            item.get("column_id") or item.get("columnId")
            for item in [*self.filters, *self.sort_params]
        }
        needs_end_time = bool(candidate_columns & {"end_time", "duration"})
        needs_cost = "total_cost" in candidate_columns
        needs_tokens = "total_tokens" in candidate_columns
        needs_traces = bool(candidate_columns & {"traces_count", "total_traces_count"})
        needs_messages = bool(candidate_columns & {"first_message", "last_message"})

        latest_metric_columns: list[str] = []
        resolved_metric_columns: list[str] = []
        session_metric_columns: list[str] = []
        root_filter_predicates: list[str] = []
        for plan in root_filter_plans:
            params.update(plan.params)
            root_filter_predicates.append(plan.predicate)
            for aggregate in plan.aggregates:
                latest_metric_columns.append(aggregate)
                alias = aggregate.rsplit(" AS ", 1)[-1].strip()
                if not alias or alias == aggregate:
                    raise ValueError("latest-state session aggregate requires an alias")
                resolved_metric_columns.append(alias)
        # A raw witness is only a candidate superset.  It narrows expensive
        # attribute argMax replay to matching physical roots; exact latest-state
        # predicates below remain authoritative.
        root_witness_plan = self._bounded_root_witness_plan(root_filter_plans)
        root_witness_predicate = (
            root_witness_plan.raw_witness_predicate if root_witness_plan else None
        )
        if needs_end_time:
            latest_metric_columns.append(
                "argMax(tuple(end_time), _peerdb_version).1 AS latest_end_time"
            )
            resolved_metric_columns.append("latest_end_time AS end_time")
            session_metric_columns.extend(
                [
                    "max(end_time) AS session_end",
                    "dateDiff('second', min(start_time), max(end_time)) AS duration",
                ]
            )
        if needs_cost:
            latest_metric_columns.append(
                "argMax(tuple(cost), _peerdb_version).1 AS latest_cost"
            )
            resolved_metric_columns.append("latest_cost AS cost")
            session_metric_columns.append("sum(cost) AS total_cost")
        if needs_tokens:
            latest_metric_columns.append(
                "argMax(tuple(total_tokens), _peerdb_version).1 AS latest_total_tokens"
            )
            resolved_metric_columns.append("latest_total_tokens AS total_tokens")
            session_metric_columns.append("sum(total_tokens) AS total_tokens")
        if needs_traces:
            resolved_metric_columns.append("trace_id")
            session_metric_columns.append("uniq(trace_id) AS traces_count")
        if needs_messages:
            latest_metric_columns.append(
                "argMax(tuple(input), _peerdb_version).1 AS latest_input"
            )
            resolved_metric_columns.append("latest_input AS input")
            session_metric_columns.extend(
                [
                    "argMin(input, start_time) AS first_message",
                    "argMax(input, start_time) AS last_message",
                ]
            )
        latest_metric_select = (
            ",\n                " + ",\n                ".join(latest_metric_columns)
            if latest_metric_columns
            else ""
        )
        resolved_metric_select = (
            ",\n                " + ",\n                ".join(resolved_metric_columns)
            if resolved_metric_columns
            else ""
        )
        session_metric_select = (
            ",\n                " + ",\n                ".join(session_metric_columns)
            if session_metric_columns
            else ""
        )

        end_time_filters = [
            item
            for item in self.filters
            if (item.get("column_id") or item.get("columnId")) == "end_time"
        ]
        root_value_clause = ""
        if end_time_filters:
            filter_builder = ClickHouseFilterBuilder(
                table=self.TABLE,
                project_id=self.project_id,
                project_ids=self.project_ids,
            )
            root_value_clause, root_value_params = filter_builder.translate(
                end_time_filters
            )
            params.update(root_value_params)

        has_explicit_time_filter = any(
            (item.get("column_id") or item.get("columnId"))
            in {"created_at", "start_time"}
            for item in self.filters
        )
        scope_to_request_window = not candidate_full_state or has_explicit_time_filter
        span_time_scope = (
            "\n              AND toDate(start_time) BETWEEN "
            "toDate(%(start_date)s) AND toDate(%(end_date)s)"
            "\n              AND start_time >= %(start_date)s"
            "\n              AND start_time < %(end_date)s"
            if scope_to_request_window
            else ""
        )
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="session_candidate_time_exclusion",
            )
        )
        params.update(datetime_params)
        if datetime_predicate:
            span_time_scope += f"\n              AND {datetime_predicate}"

        positive_session_ids = self._candidate_positive_filter_values(
            self._SESSION_ID_FILTER_COLS
        )
        seed_session_ids = candidate_session_ids or positive_session_ids
        ts_map_ctes = (
            f"ts_survivor_map AS ({survivor_map_subquery('trace_session_id_remap')})"
        )
        candidate_session_cte = ""
        root_session_seed = ""
        if seed_session_ids:
            params["candidate_filter_session_ids"] = seed_session_ids
            # A seed may carry any member of a consolidation group: its
            # survivor old ID, a non-survivor old ID, or the deterministic new
            # ID. Resolve the finite old-ID side by primary key, reverse the
            # resulting/new input IDs in one authoritative pass, and materialize
            # that tiny map once. This preserves the exact many-old→one-new
            # survivor rule without repeating the non-key scan at every CTE use.
            ts_map_ctes = self._candidate_survivor_map_ctes(params, seed_session_ids)
            resolved_candidate_session = resolved_id_expr(
                "candidate_raw_session_id", "candidate_ts_remap"
            )
            candidate_session_cte = f""",
        candidate_filter_sessions AS (
            SELECT DISTINCT {resolved_candidate_session} AS session_id
            FROM (
                SELECT arrayJoin(
                    CAST(%(candidate_filter_session_id_array)s AS Array(UUID))
                ) AS candidate_raw_session_id
            ) AS candidate_raw_sessions
            LEFT JOIN ts_survivor_map AS candidate_ts_remap
                ON candidate_raw_session_id = candidate_ts_remap.any_id
        )"""
            root_session_seed = """
              AND (
                  trace_session_id IN (
                      SELECT session_id FROM candidate_filter_sessions
                  )
                  OR trace_session_id IN (
                      SELECT any_id
                      FROM ts_survivor_map
                      WHERE survivor_id IN (
                          SELECT session_id FROM candidate_filter_sessions
                      )
                  )
              )
            """

        user_filter_items = [
            item
            for item in self.filters
            if (item.get("column_id") or item.get("columnId"))
            in self._ENDUSER_ID_FILTER_COLS
        ]
        user_ids: list[str] = []
        for item in user_filter_items:
            config = item.get("filter_config") or item.get("filterConfig") or {}
            raw = config.get("filter_value", config.get("filterValue"))
            values = raw if isinstance(raw, list) else [raw]
            user_ids.extend(str(value) for value in values if value)
        if self.user_id:
            user_ids.append(str(self.user_id))
        user_ids = list(dict.fromkeys(user_ids))
        user_null_op = self._user_null_filter_op()
        resolved_user_clause = (
            "" if user_null_op else self._build_resolved_user_clause(params)
        )
        user_ctes = ""
        user_membership = ""
        eu_map_cte = ""
        if resolved_user_clause or user_null_op:
            eu_map = survivor_map_subquery("end_user_id_remap")
            resolved_user_session = resolved_id_expr(
                "latest_trace_session_id", "user_ts_remap"
            )
            resolved_user = resolved_id_expr("latest_end_user_id", "user_eu_remap")
            eu_map_cte = f",\n        eu_survivor_map AS ({eu_map})"

            # Page-list user filters are positive and can seed by requested
            # user IDs.  The bounded bulk classifier already has <=200 session
            # IDs, so it scopes by those IDs instead; this also makes NOT IN and
            # null-presence semantics exact without scanning every user span in
            # the project.
            user_filter_ops = {
                (item.get("filter_config") or item.get("filterConfig") or {}).get(
                    "filter_op"
                )
                or (item.get("filter_config") or item.get("filterConfig") or {}).get(
                    "filterOp"
                )
                for item in user_filter_items
            }
            positive_user_seed = user_filter_ops <= {"equals", "in"}
            if candidate_session_ids:
                user_seed_clause = """
              AND (
                  trace_session_id IN (
                      SELECT session_id FROM candidate_filter_sessions
                  )
                  OR trace_session_id IN (
                      SELECT any_id
                      FROM ts_survivor_map
                      WHERE survivor_id IN (
                          SELECT session_id FROM candidate_filter_sessions
                      )
                  )
              )
                """
            elif positive_user_seed:
                # A positive filter with an empty value list must match nothing,
                # never widen into an unscoped scan.
                params["candidate_filter_user_ids"] = tuple(user_ids or [NIL_UUID])
                user_seed_clause = """
              AND (
                  end_user_id IN %(candidate_filter_user_ids)s
                  OR end_user_id IN (
                      SELECT any_id
                      FROM eu_survivor_map
                      WHERE survivor_id IN %(candidate_filter_user_ids)s
                  )
              )
                """
            else:
                # NOT IN / null-presence predicates cannot be seeded by the
                # requested user IDs without changing their meaning.  The scan
                # remains project/time/root-column only and replays latest
                # physical state before forming session membership.
                user_seed_clause = ""
            user_ctes = f""",
        candidate_user_span_identities AS (
            SELECT DISTINCT project_id, trace_id, id, start_time
            FROM {self.TABLE}
            PREWHERE {self.project_filter_sql()}{span_time_scope}
              {user_seed_clause}
        ),
        latest_user_spans AS (
            SELECT
                project_id,
                trace_id,
                id,
                start_time,
                argMax(tuple(trace_session_id), _peerdb_version).1 AS latest_trace_session_id,
                argMax(tuple(end_user_id), _peerdb_version).1 AS latest_end_user_id,
                argMax(_peerdb_is_deleted, _peerdb_version) AS latest_is_deleted
            FROM {self.TABLE}
            PREWHERE {self.project_filter_sql()}{span_time_scope}
              AND (project_id, trace_id, id, start_time) IN (
                  SELECT project_id, trace_id, id, start_time
                  FROM candidate_user_span_identities
              )
            GROUP BY project_id, trace_id, id, start_time
        ),
        resolved_user_spans AS (
            SELECT
                {resolved_user_session} AS session_id,
                {resolved_user} AS end_user_id
            FROM latest_user_spans
            LEFT JOIN ts_survivor_map AS user_ts_remap
                ON latest_trace_session_id = user_ts_remap.any_id
            LEFT JOIN eu_survivor_map AS user_eu_remap
                ON latest_end_user_id = user_eu_remap.any_id
            WHERE latest_is_deleted = 0
              AND isNotNull(latest_trace_session_id)
              AND latest_trace_session_id != toUUID('{NIL_UUID}')
        ),
        matching_user_sessions AS (
            SELECT session_id
            FROM resolved_user_spans
            WHERE isNotNull(end_user_id)
              AND end_user_id != toUUID('{NIL_UUID}')
              {f"AND {resolved_user_clause}" if resolved_user_clause else ""}
            GROUP BY session_id
        )"""
            membership_op = "NOT IN" if user_null_op == "is_null" else "IN"
            user_membership = (
                f"AND session_id {membership_op} "
                "(SELECT session_id FROM matching_user_sessions)"
            )

        session_predicate = (
            f"AND {resolved_session_clause}" if resolved_session_clause else ""
        )
        return f"""
        {ts_map_ctes}
        {candidate_session_cte}
        {eu_map_cte},
        candidate_root_identities AS (
            SELECT DISTINCT project_id, trace_id, id, start_time
            FROM {self.TABLE}
            PREWHERE {self.project_filter_sql()}{span_time_scope}
            WHERE (parent_span_id IS NULL OR parent_span_id = '')
              {root_session_seed}
              {f"AND ({root_witness_predicate})" if root_witness_predicate else ""}
        ),
        latest_roots AS (
            SELECT
                project_id,
                trace_id,
                id,
                start_time,
                argMax(tuple(parent_span_id), _peerdb_version).1 AS latest_parent_span_id,
                argMax(tuple(trace_session_id), _peerdb_version).1 AS latest_trace_session_id,
                argMax(_peerdb_is_deleted, _peerdb_version) AS latest_is_deleted
                {latest_metric_select}
            FROM {self.TABLE}
            PREWHERE {self.project_filter_sql()}{span_time_scope}
              AND (project_id, trace_id, id, start_time) IN (
                  SELECT project_id, trace_id, id, start_time
                  FROM candidate_root_identities
              )
            GROUP BY project_id, trace_id, id, start_time
        ),
        resolved_root_sessions AS (
            SELECT
                {resolved_session} AS session_id,
                start_time
                {resolved_metric_select}
            FROM latest_roots
            LEFT JOIN ts_survivor_map AS ts_remap
                ON latest_trace_session_id = ts_remap.any_id
            WHERE latest_is_deleted = 0
              AND (latest_parent_span_id IS NULL OR latest_parent_span_id = '')
              AND isNotNull(latest_trace_session_id)
              AND latest_trace_session_id != toUUID('{NIL_UUID}')
              {f"AND {root_value_clause}" if root_value_clause else ""}
              {f"AND {' AND '.join(root_filter_predicates)}" if root_filter_predicates else ""}
        )
        {user_ctes},
        sessions AS (
            SELECT
                session_id,
                min(start_time) AS session_start
                {session_metric_select}
            FROM resolved_root_sessions
            WHERE 1 = 1
              {session_predicate}
              {user_membership}
            GROUP BY session_id
            {f"HAVING {aggregate_clause}" if aggregate_clause else ""}
        )
        """

    def build_filter_seed_page(
        self,
        *,
        slice_start: datetime,
        slice_end: datetime,
        limit: int,
        before_start_time: datetime | None = None,
        before_id: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Seed a finite newest-first superset of session IDs.

        This query deliberately reads only raw root identity/order columns.
        Newer tombstones may create false-positive seeds, but cannot hide a
        live session; ``build_filter_match_query`` replays latest state before
        admitting any seed into the result.
        """

        if not self.supports_bounded_filter_scan():
            raise ValueError("unsupported bounded session filter scan")
        # Seed rows contain only an identity/order tuple. Normal list reads use
        # raw session IDs so every adjacent slice avoids a broad FINAL remap;
        # the finite classifier resolves each raw ID to its survivor and expands
        # the complete consolidation group exactly once. Eval sampling retains
        # its canonical-ID hash contract below. Population proofs may acquire
        # the shared 512-row working set, while callers split exact latest-state
        # replay using the smaller recommended classifier batch.
        if limit <= 0 or limit > 512:
            raise ValueError("session seed limit must be between 1 and 512")
        if (before_start_time is None) != (before_id is None):
            raise ValueError("session keyset values must be provided together")
        request_start, request_end = self.parse_time_range(self.filters)
        if not request_start <= slice_start < slice_end <= request_end:
            raise ValueError("session seed slice must stay inside the request window")

        params: dict[str, Any] = {
            **self.params,
            "filter_slice_start": slice_start,
            "filter_slice_end": slice_end,
            "filter_seed_limit": int(limit),
        }
        plans, residual = self._bounded_span_filter_parts()
        if residual:
            raise ValueError("unsupported relational bounded session filter")
        root_witness_plan = self._bounded_root_witness_plan(plans)
        root_witness_predicate = (
            root_witness_plan.raw_witness_predicate if root_witness_plan else None
        )
        if root_witness_plan is not None:
            params.update(
                {
                    key: value
                    for key, value in root_witness_plan.params.items()
                    if f"%({key})s" in root_witness_predicate
                }
            )
        root_witness_clause = (
            f"\n                  AND ({root_witness_predicate})"
            if root_witness_predicate
            else ""
        )
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="session_seed_time_exclusion",
            )
        )
        # The helper deliberately accepts only a bare identifier. Qualify its
        # trusted output at the call site so ClickHouse's analyzer cannot
        # substitute the outer ``max(...) AS start_time`` aggregate alias back
        # into this physical-row predicate.
        datetime_predicate = datetime_predicate.replace(
            "start_time", "seed_spans.start_time"
        )
        params.update(datetime_params)
        datetime_fragment = (
            f"\n                  AND {datetime_predicate}"
            if datetime_predicate
            else ""
        )
        outer_predicates: list[str] = []
        if before_start_time is not None:
            if not slice_start <= before_start_time < slice_end:
                raise ValueError("session keyset must stay inside its slice")
            params["filter_before_start_time"] = before_start_time
            params["filter_before_session_id"] = str(before_id)
            outer_predicates.append(
                "(start_time < %(filter_before_start_time)s OR ("
                "start_time = %(filter_before_start_time)s AND "
                "toString(session_id) < %(filter_before_session_id)s))"
            )
        if self._bounded_sampling_rate is not None:
            params["bounded_sampling_salt"] = str(self._bounded_sampling_salt)
            params["bounded_sampling_rate"] = float(self._bounded_sampling_rate)
            outer_predicates.append(
                "modulo(cityHash64(%(bounded_sampling_salt)s, "
                "toString(session_id)), 100) < %(bounded_sampling_rate)s"
            )
        outer_where = (
            f"WHERE {' AND '.join(outer_predicates)}" if outer_predicates else ""
        )

        seed_source = f"""
            SELECT
                seed_spans.trace_session_id AS session_id,
                max(seed_spans.start_time) AS start_time
            FROM {self.TABLE} AS seed_spans
            PREWHERE {self.project_filter_sql()}
              AND seed_spans._peerdb_is_deleted = 0
              AND seed_spans.start_time >= %(filter_slice_start)s
              AND seed_spans.start_time < %(filter_slice_end)s{datetime_fragment}
            WHERE (seed_spans.parent_span_id IS NULL OR seed_spans.parent_span_id = '')
              AND isNotNull(seed_spans.trace_session_id)
              AND seed_spans.trace_session_id != toUUID('{NIL_UUID}'){root_witness_clause}
            GROUP BY seed_spans.trace_session_id
        """
        if self._bounded_sampling_rate is not None:
            # Sampling is defined on the canonical public session ID. Keep the
            # pre-existing remap only for this internal sampled lane; moving the
            # hash to raw aliases would change which straddlers are selected.
            ts_map = survivor_map_subquery("trace_session_id_remap")
            resolved_session = resolved_id_expr("raw_trace_session_id", "seed_ts_remap")
            seed_source = f"""
            SELECT
                {resolved_session} AS session_id,
                max(start_time) AS start_time
            FROM (
                SELECT
                    seed_spans.trace_session_id AS raw_trace_session_id,
                    seed_spans.start_time AS start_time
                FROM {self.TABLE} AS seed_spans
                PREWHERE {self.project_filter_sql()}
                  AND seed_spans._peerdb_is_deleted = 0
                  AND seed_spans.start_time >= %(filter_slice_start)s
                  AND seed_spans.start_time < %(filter_slice_end)s{datetime_fragment}
                WHERE (seed_spans.parent_span_id IS NULL OR seed_spans.parent_span_id = '')
                  AND isNotNull(seed_spans.trace_session_id)
                  AND seed_spans.trace_session_id != toUUID('{NIL_UUID}'){root_witness_clause}
            ) AS raw_roots
            LEFT JOIN ({ts_map}) AS seed_ts_remap
                ON raw_trace_session_id = seed_ts_remap.any_id
            GROUP BY session_id
            """
        query = f"""
        WITH seed_sessions AS (
            {seed_source}
        )
        SELECT session_id, start_time
        FROM seed_sessions
        {outer_where}
        ORDER BY start_time DESC, toString(session_id) DESC
        LIMIT %(filter_seed_limit)s
        """
        return query, params

    def build_filter_match_query(
        self,
        candidate_ids: list[str],
        *,
        candidate_full_state: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        """Classify at most 200 session IDs against exact latest root state."""

        session_ids = tuple(
            dict.fromkeys(str(value) for value in candidate_ids if value)
        )
        if not session_ids:
            return "", {}
        if len(session_ids) > 200:
            raise ValueError("candidate session batch exceeds bounded limit")
        if not self.supports_bounded_filter_scan():
            raise ValueError("unsupported bounded session filter scan")

        plans, residual = self._bounded_span_filter_parts()
        if residual:
            raise ValueError("unsupported relational bounded session filter")
        start_date, end_date = self.parse_time_range(self.filters)
        params: dict[str, Any] = {
            **self.params,
            "start_date": start_date,
            "end_date": end_date,
            "bounded_match_limit": len(session_ids),
        }
        candidate_ctes = self._candidate_session_ctes(
            params,
            candidate_session_ids=session_ids,
            root_filter_plans=tuple(plans),
            candidate_full_state=candidate_full_state,
        )
        query = f"""
        WITH
        {candidate_ctes}
        SELECT session_id, session_start AS start_time
        FROM sessions
        ORDER BY start_time DESC, toString(session_id) DESC
        LIMIT %(bounded_match_limit)s
        """
        return query, params

    def build_candidate_page_query(self) -> tuple[str, dict[str, Any]]:
        """Select only the exact session page from physical latest root spans.

        Unlike the historical query, this first pass does not read cost/token/
        enrichment columns for every session. It replays the direct-write
        physical identity ``(project, trace, span id, start_time)`` with
        latest-wins tombstone semantics. When membership or ordering depends on
        aggregate/message metrics, only those narrow root columns are computed
        before the page; full content/attribute enrichment remains page-scoped.
        """

        if not self.supports_candidate_first_page():
            raise ValueError("session request is not candidate-page safe")
        self.start_date, self.end_date = self.parse_time_range(self.filters)
        params = {
            **self.params,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "limit": self.page_size + 1,
            "offset": self.page_number * self.page_size,
        }
        candidate_ctes = self._candidate_session_ctes(params)
        order_clause = self._candidate_order_clause()
        query = f"""
        WITH
        {candidate_ctes}
        SELECT
            session_id,
            session_start,
            count() OVER() AS total_count
        FROM sessions
        {order_clause}
        LIMIT %(limit)s
        OFFSET %(offset)s
        """
        return query, params

    def build_candidate_count_query(self) -> tuple[str, dict[str, Any]]:
        """Count exact candidate sessions when an out-of-range page is empty.

        ``count() OVER()`` has no carrier row on an empty page.  This fallback
        repeats only the physical identity/latest-state selector; it does not
        run the historical content/cost/token aggregation.
        """

        if not self.supports_candidate_first_page():
            raise ValueError("session request is not candidate-page safe")
        start_date, end_date = self.parse_time_range(self.filters)
        params = {
            **self.params,
            "start_date": start_date,
            "end_date": end_date,
        }
        candidate_ctes = self._candidate_session_ctes(params)
        query = f"""
        WITH
        {candidate_ctes}
        SELECT count() AS total
        FROM sessions
        """
        return query, params

    def build_page_metrics_query(
        self, session_ids: list[str]
    ) -> tuple[str, dict[str, Any]]:
        """Hydrate aggregates for an already selected <=200-session page."""

        ids = tuple(dict.fromkeys(str(value) for value in session_ids if value))
        if not ids:
            return "", {}
        if len(ids) > 200:
            raise ValueError("candidate session page exceeds bounded limit")
        start_date, end_date = self.parse_time_range(self.filters)
        params = {
            **self.params,
            "start_date": start_date,
            "end_date": end_date,
            "candidate_session_ids": ids,
        }
        ts_map = survivor_map_subquery("trace_session_id_remap")
        resolved_session = resolved_id_expr("latest_trace_session_id", "ts_remap")
        query = f"""
        WITH
        ts_survivor_map AS ({ts_map}),
        candidate_root_identities AS (
            SELECT DISTINCT
                project_id,
                trace_id,
                id,
                start_time
            FROM {self.TABLE}
            PREWHERE {self.project_filter_sql()}
              AND toDate(start_time) BETWEEN toDate(%(start_date)s) AND toDate(%(end_date)s)
              AND start_time >= %(start_date)s
              AND start_time < %(end_date)s
              AND (
                  trace_session_id IN %(candidate_session_ids)s
                  OR trace_session_id IN (
                      SELECT any_id
                      FROM ts_survivor_map
                      WHERE survivor_id IN %(candidate_session_ids)s
                  )
              )
              AND (parent_span_id IS NULL OR parent_span_id = '')
        ),
        latest_roots AS (
            SELECT
                project_id,
                trace_id,
                id,
                start_time,
                argMax(tuple(parent_span_id), _peerdb_version).1 AS latest_parent_span_id,
                argMax(tuple(trace_session_id), _peerdb_version).1 AS latest_trace_session_id,
                argMax(tuple(end_time), _peerdb_version).1 AS latest_end_time,
                argMax(tuple(cost), _peerdb_version).1 AS latest_cost,
                argMax(tuple(total_tokens), _peerdb_version).1 AS latest_total_tokens,
                argMax(_peerdb_is_deleted, _peerdb_version) AS latest_is_deleted
            FROM {self.TABLE}
            PREWHERE {self.project_filter_sql()}
              AND toDate(start_time) BETWEEN toDate(%(start_date)s) AND toDate(%(end_date)s)
              AND start_time >= %(start_date)s
              AND start_time < %(end_date)s
              AND (project_id, trace_id, id, start_time) IN (
                  SELECT project_id, trace_id, id, start_time
                  FROM candidate_root_identities
              )
            GROUP BY project_id, trace_id, id, start_time
        ),
        resolved_roots AS (
            SELECT
                {resolved_session} AS session_id,
                trace_id,
                start_time,
                latest_end_time AS end_time,
                latest_cost AS cost,
                latest_total_tokens AS total_tokens
            FROM latest_roots
            LEFT JOIN ts_survivor_map AS ts_remap
                ON latest_trace_session_id = ts_remap.any_id
            WHERE latest_is_deleted = 0
              AND (latest_parent_span_id IS NULL OR latest_parent_span_id = '')
              AND {resolved_session} IN %(candidate_session_ids)s
        )
        SELECT
            session_id,
            min(start_time) AS session_start,
            max(end_time) AS session_end,
            dateDiff('second', min(start_time), max(end_time)) AS duration,
            sum(cost) AS total_cost,
            sum(total_tokens) AS total_tokens,
            uniq(trace_id) AS traces_count
        FROM resolved_roots
        GROUP BY session_id
        """
        return query, params

    def build(self) -> tuple[str, dict[str, Any]]:
        """Build the session list query.

        Returns:
            A ``(query_string, params)`` tuple.
        """
        self.start_date, self.end_date = self.parse_time_range(self.filters)
        self.params["start_date"] = self.start_date
        self.params["end_date"] = self.end_date

        # Translate span-level filters (exclude session-level aggregate
        # filters AND end_user_id filters handled via subquery)
        span_filters = self._extract_span_filters()
        fb = ClickHouseFilterBuilder(
            table=self.TABLE,
            project_id=self.project_id,
            project_ids=self.project_ids,
        )
        extra_where, extra_params = fb.translate(span_filters)
        self.params.update(extra_params)

        # Build HAVING clauses for aggregate-level filters
        having_clauses = self._build_having_clauses()

        # Sorting
        order_clause = fb.translate_sort(
            self.sort_params, field_map=self.SORT_FIELD_MAP
        )
        if not order_clause:
            order_clause = "ORDER BY session_start DESC"

        # Pagination
        offset = self.page_number * self.page_size
        self.params["limit"] = self.page_size + 1  # +1 for has_more
        self.params["offset"] = offset

        # Optional user filter (legacy path via self.user_id kwarg)
        if self.user_id:
            self.params["user_id"] = self.user_id

        filter_fragment = f"AND {extra_where}" if extra_where else ""
        having_fragment = f"HAVING {having_clauses}" if having_clauses else ""
        message_select = self._message_aggregate_select()

        # Resolve session IDs new→old before grouping so cross-cutover spans
        # remain one session. User membership is handled separately below.
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="session_list_time_exclusion",
            )
        )
        self.params.update(datetime_params)
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )
        time_where = (
            "AND start_time >= %(start_date)s AND start_time < %(end_date)s"
            f"{datetime_fragment}"
        )
        from_where = self._session_from_where(
            self.params,
            time_where=time_where,
            filter_fragment=filter_fragment,
        )

        # Keep the common path light. Message aggregates are added only when
        # first_message/last_message participate in filtering.
        query = f"""
        SELECT
            trace_session_id AS session_id,
            {self._AGGREGATE_SELECT}
            {message_select}
        {from_where}
        GROUP BY trace_session_id
        {having_fragment}
        {order_clause}
        LIMIT %(limit)s
        OFFSET %(offset)s
        """
        return query, self.params

    def build_id_query(
        self,
        *,
        created_at_floor: datetime | None = None,
        created_at_ceiling: datetime | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Filtered session ids only — same grouped, remap-aware scan as build(),
        no pagination/order. Lets the eval resolver select the same sessions this
        list endpoint returns.

        ``created_at_floor`` (continuous eval tasks only): floor the span scan on
        CH arrival time (``created_at``) instead of event time (``start_time``),
        so a session whose spans landed in CH after their ``start_time`` is still
        picked up. ``None`` keeps the ``start_time`` window used by the UI list
        and historical tasks.
        """
        self.start_date, self.end_date = self.parse_time_range(self.filters)
        self.params["start_date"] = self.start_date
        self.params["end_date"] = self.end_date

        span_filters = self._extract_span_filters()
        fb = ClickHouseFilterBuilder(
            table=self.TABLE,
            project_id=self.project_id,
            project_ids=self.project_ids,
        )
        extra_where, extra_params = fb.translate(span_filters)
        self.params.update(extra_params)

        having_clauses = self._build_having_clauses()
        if self.user_id:
            self.params["user_id"] = self.user_id

        filter_fragment = f"AND {extra_where}" if extra_where else ""
        having_fragment = f"HAVING {having_clauses}" if having_clauses else ""
        message_select = self._message_aggregate_select()
        if created_at_floor is not None:
            # Window on arrival (created_at), not start_time. NOTE: with a user
            # filter, the membership subqueries still window on start_time, so a
            # filtered task can miss an arrival whose start_time predates
            # parse_time_range's window — pre-existing residual, tracked as a
            # follow-up.
            self.params["created_at_floor"] = created_at_floor
            time_where = "AND created_at >= %(created_at_floor)s"
            if created_at_ceiling is not None:
                self.params["created_at_ceiling"] = created_at_ceiling
                time_where += " AND created_at < %(created_at_ceiling)s"
        else:
            time_where = (
                "AND start_time >= %(start_date)s AND start_time < %(end_date)s"
            )
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="session_id_time_exclusion",
            )
        )
        self.params.update(datetime_params)
        if datetime_predicate:
            time_where += f"\n          AND {datetime_predicate}"
        from_where = self._session_from_where(
            self.params,
            time_where=time_where,
            filter_fragment=filter_fragment,
        )

        query = f"""
        SELECT
            trace_session_id AS session_id,
            {self._AGGREGATE_SELECT}
            {message_select}
        {from_where}
        GROUP BY trace_session_id
        {having_fragment}
        """
        return query, self.params

    def build_content_query(self, session_ids: list[str]) -> tuple[str, dict[str, Any]]:
        """Fetch first/last messages for a page of session IDs.

        P3b step1.5 (DESIGN §3 / id_remap_sql): ``session_ids`` are the OLD
        curated ids emitted by the (resolved) browse ``build()``. A straddler's
        NEW-deterministic-id spans carry ``trace_session_id = new_id``, so we
        resolve each span's ``trace_session_id`` new→old through
        ``trace_session_id_remap`` and BOTH filter (``IN session_ids``) and
        ``GROUP BY`` the RESOLVED id — else the new-id spans are missed and a
        straddler's first/last message is computed off only its old-id half.
        Pre-flip the remap is a no-op → byte-identical (gate B).
        """
        ids = tuple(dict.fromkeys(str(value) for value in session_ids if value))
        if not ids:
            return "", {}
        if len(ids) > 200:
            raise ValueError("content session page exceeds bounded limit")
        # The bounded endpoint calls this method without calling ``build`` first.
        # Derive its exact request window here so both the raw candidate read and
        # four-field latest-state replay can prune the start_time partitions.
        content_start_date, content_end_date = self.parse_time_range(self.filters)
        params = {
            **self.params,
            "content_session_ids": ids,
            "content_start_date": content_start_date,
            "content_end_date": content_end_date,
        }
        content_exclusion, content_exclusion_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="session_content_time_exclusion",
            )
        )
        params.update(content_exclusion_params)
        content_exclusion_fragment = (
            f"\n              AND {content_exclusion}" if content_exclusion else ""
        )
        # The page contains at most 200 canonical session IDs.  Building the
        # global survivor map here scans ``trace_session_id_remap`` twice even
        # though hydration can only return those finite sessions.  Expand only
        # their consolidation groups and materialize the finite map once,
        # preserving the identical old/new -> survivor mapping while keeping
        # the span read page-scoped.
        # An unmapped direct-CH session still follows the explicit raw-ID arm
        # below and ``resolved_id_expr`` falls back to that raw ID.
        ts_map_ctes = self._candidate_survivor_map_ctes(params, ids)
        resolved_ts = resolved_id_expr("latest_trace_session_id", "ts_remap")
        query = f"""
        WITH
        {ts_map_ctes},
        candidate_root_identities AS (
            SELECT DISTINCT project_id, trace_id, id, start_time
            FROM {self.TABLE}
            PREWHERE {self.project_filter_sql()}
              AND toDate(start_time) BETWEEN
                  toDate(%(content_start_date)s) AND toDate(%(content_end_date)s)
              AND start_time >= %(content_start_date)s
              AND start_time < %(content_end_date)s{content_exclusion_fragment}
              AND (
                  trace_session_id IN %(content_session_ids)s
                  OR trace_session_id IN (
                      SELECT any_id
                      FROM ts_survivor_map
                      WHERE survivor_id IN %(content_session_ids)s
                  )
              )
              AND (parent_span_id IS NULL OR parent_span_id = '')
        ),
        latest_roots AS (
            SELECT
                project_id,
                trace_id,
                id,
                start_time,
                argMax(tuple(parent_span_id), _peerdb_version).1 AS latest_parent_span_id,
                argMax(tuple(trace_session_id), _peerdb_version).1 AS latest_trace_session_id,
                argMax(tuple(input), _peerdb_version).1 AS latest_input,
                argMax(_peerdb_is_deleted, _peerdb_version) AS latest_is_deleted
            FROM {self.TABLE}
            PREWHERE {self.project_filter_sql()}
              AND toDate(start_time) BETWEEN
                  toDate(%(content_start_date)s) AND toDate(%(content_end_date)s)
              AND start_time >= %(content_start_date)s
              AND start_time < %(content_end_date)s{content_exclusion_fragment}
              AND (project_id, trace_id, id, start_time) IN (
                  SELECT project_id, trace_id, id, start_time
                  FROM candidate_root_identities
              )
            GROUP BY project_id, trace_id, id, start_time
        ),
        resolved_roots AS (
            SELECT
                {resolved_ts} AS session_id,
                start_time,
                latest_input AS input
            FROM latest_roots
            LEFT JOIN ts_survivor_map AS ts_remap
                ON latest_trace_session_id = ts_remap.any_id
            WHERE latest_is_deleted = 0
              AND (latest_parent_span_id IS NULL OR latest_parent_span_id = '')
              AND {resolved_ts} IN %(content_session_ids)s
        )
        SELECT
            session_id,
            argMin(input, start_time) AS first_message,
            argMax(input, start_time) AS last_message
        FROM resolved_roots
        GROUP BY session_id
        """
        return query, params

    def has_having_filters(self) -> bool:
        """Return True if any filters target aggregate columns (requiring HAVING)."""
        for f in self.filters:
            col_id = f.get("column_id") or f.get("columnId")
            if col_id in self.SESSION_FILTER_MAP or col_id in self.MESSAGE_FILTER_MAP:
                return True
        return False

    def build_count_query(self) -> tuple[str, dict[str, Any]]:
        """Build a query to count total matching sessions (for pagination).

        Uses a fast ``count(DISTINCT ...)`` path when no HAVING clauses are
        needed, and falls back to the full aggregation subquery when aggregate
        filters (duration, cost, tokens, traces_count) are present.

        Returns:
            A ``(query_string, params)`` tuple returning a single count.
        """
        if not self.has_having_filters():
            return self._build_simple_count_query()
        return self._build_aggregated_count_query()

    def _build_simple_count_query(self) -> tuple[str, dict[str, Any]]:
        """Fast count using count(DISTINCT ...) — no GROUP BY needed."""
        span_filters = self._extract_span_filters()
        fb = ClickHouseFilterBuilder(
            table=self.TABLE,
            project_id=self.project_id,
            project_ids=self.project_ids,
        )
        extra_where, extra_params = fb.translate(span_filters)

        params = dict(self.params)
        params.update(extra_params)

        filter_fragment = f"AND {extra_where}" if extra_where else ""

        # P3b step1.5: same id-remap-resolved scan as build() (trace_session_id
        # always, end_user_id when filtered) so `count(DISTINCT trace_session_id)`
        # unifies a straddler and the count matches the listed rows (else
        # has_more/pagination lies). Pre-flip a byte-identical no-op (gate B).
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="session_simple_count_time_exclusion",
            )
        )
        params.update(datetime_params)
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )
        time_where = (
            "AND start_time >= %(start_date)s AND start_time < %(end_date)s"
            f"{datetime_fragment}"
        )
        from_where = self._session_from_where(
            params,
            time_where=time_where,
            filter_fragment=filter_fragment,
        )

        query = f"""
        SELECT count(DISTINCT trace_session_id) AS total
        {from_where}
        """
        return query, params

    def _build_aggregated_count_query(self) -> tuple[str, dict[str, Any]]:
        """Full aggregation count — required when HAVING clauses exist."""
        span_filters = self._extract_span_filters()
        fb = ClickHouseFilterBuilder(
            table=self.TABLE,
            project_id=self.project_id,
            project_ids=self.project_ids,
        )
        extra_where, extra_params = fb.translate(span_filters)

        params = dict(self.params)
        params.update(extra_params)

        having_clauses = self._build_having_clauses()

        filter_fragment = f"AND {extra_where}" if extra_where else ""
        having_fragment = f"HAVING {having_clauses}" if having_clauses else ""
        message_select = self._message_aggregate_select()

        # P3b step1.5: same id-remap-resolved scan as build()/simple-count so the
        # HAVING-filtered session count unifies a straddler identically (group on
        # the resolved trace_session_id). Pre-flip a byte-identical no-op (gate B).
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="session_aggregate_count_time_exclusion",
            )
        )
        params.update(datetime_params)
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )
        time_where = (
            "AND start_time >= %(start_date)s AND start_time < %(end_date)s"
            f"{datetime_fragment}"
        )
        from_where = self._session_from_where(
            params,
            time_where=time_where,
            filter_fragment=filter_fragment,
        )

        # Select the aggregate aliases so HAVING on `duration`/`total_cost`/
        # `total_tokens`/`traces_count` resolves (otherwise CH raises Code 47
        # "Unknown expression identifier" — TH-4316).
        query = f"""
        SELECT count() AS total FROM (
            SELECT
                trace_session_id,
                dateDiff('second', min(start_time), max(end_time)) AS duration,
                sum(cost) AS total_cost,
                sum(total_tokens) AS total_tokens,
                uniq(trace_id) AS traces_count
                {message_select}
            {from_where}
            GROUP BY trace_session_id
            {having_fragment}
        )
        """
        return query, params

    def build_span_attributes_query(
        self, session_ids: list[str]
    ) -> tuple[str, dict[str, Any]]:
        """Fetch span attributes for root spans belonging to the given sessions.

        Restricts to root spans only (where custom user-defined attributes
        are typically set) and caps results at 500 rows to prevent unbounded
        scans on sessions with many traces.

        Returns one row per root span with trace_session_id,
        span_attributes_raw, and typed Map columns (span_attr_str,
        span_attr_num) as fallback when the raw JSON blob is empty.
        """
        if not session_ids:
            return "", {}

        # Preserve the raw session-ID prefilter while adding the list request's
        # finite partition window; this legacy method does not replay versions.
        attr_start_date, attr_end_date = self.parse_time_range(self.filters)
        params = {
            **self.params,
            "attr_session_ids": tuple(session_ids),
            "attr_start_date": attr_start_date,
            "attr_end_date": attr_end_date,
        }
        attr_exclusion, attr_exclusion_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="session_attr_time_exclusion",
            )
        )
        params.update(attr_exclusion_params)
        attr_exclusion = attr_exclusion.replace("start_time", "s.start_time")
        attr_exclusion_fragment = (
            f"\n          AND {attr_exclusion}" if attr_exclusion else ""
        )
        # P3b step1.5 (DESIGN §3 / id_remap_sql): `session_ids` are OLD curated ids
        # from the resolved browse; resolve each span's `trace_session_id` new→old
        # so a straddler's NEW-id spans' attributes attach to the OLD session id
        # the page lists. Filter + project the RESOLVED id. Pre-flip: no-op (gate
        # B). The committed PREWHERE micro-opt becomes a WHERE (the id-remap join
        # dominates the cost at scale anyway).
        #
        # Single-level SELECT (NOT a nested re-projection): the v1→v2 rewrite turns
        # bare `span_attributes_raw` into `toJSONString(attributes_extra) AS
        # span_attributes_raw`; a `<alias>.span_attributes_raw` reference would be
        # mangled by that bare-token rewrite. So the JSON/Map attribute columns
        # stay BARE (CH binds them to `s` — the remap join has no such columns),
        # and only `trace_session_id` is read prefixed as `s.trace_session_id`
        # (not a rewrite-special token) to feed the resolve expression.
        ts_join = remap_left_join(
            "s.trace_session_id", "trace_session_id_remap", "ts_remap"
        )
        resolved_ts = resolved_id_expr("s.trace_session_id", "ts_remap")
        query = f"""
        SELECT
            {resolved_ts} AS session_id,
            span_attributes_raw,
            span_attr_str,
            span_attr_num
        FROM {self.TABLE} AS s
        {ts_join}
        WHERE {self.project_filter_sql()}
          AND is_deleted = 0
          AND toDate(s.start_time) BETWEEN
              toDate(%(attr_start_date)s) AND toDate(%(attr_end_date)s)
          AND s.start_time >= %(attr_start_date)s
          AND s.start_time < %(attr_end_date)s{attr_exclusion_fragment}
          AND (parent_span_id IS NULL OR parent_span_id = '')
          AND s.trace_session_id IN %(attr_session_ids)s
          AND (
            (span_attributes_raw != '{{}}' AND span_attributes_raw != '')
            OR length(mapKeys(span_attr_str)) > 0
            OR length(mapKeys(span_attr_num)) > 0
          )
          AND {resolved_ts} IN %(attr_session_ids)s
        LIMIT 500
        """
        return query, params

    # ------------------------------------------------------------------
    # Result formatting
    # ------------------------------------------------------------------

    @staticmethod
    def format_sessions(
        rows: list[tuple],
        columns: list[str],
    ) -> list[dict[str, Any]]:
        """Convert ClickHouse rows to the session list response format.

        Args:
            rows: Raw rows from ClickHouse (dicts or tuples).
            columns: Column names.

        Returns:
            List of session dicts matching the frontend's expected shape.
        """
        results: list[dict[str, Any]] = []
        col_idx = {name: i for i, name in enumerate(columns)}

        def _get(row, key, idx, default=None):
            if isinstance(row, dict):
                return row.get(key, default)
            return (
                row[col_idx.get(key, idx)]
                if len(row) > col_idx.get(key, idx)
                else default
            )

        for row in rows:
            session_id = str(_get(row, "session_id", 0, ""))
            if session_id == NIL_UUID:
                continue
            session_start = _get(row, "session_start", 1)
            session_end = _get(row, "session_end", 2)
            duration_val = _get(row, "duration", 3, 0)

            results.append(
                {
                    "session_id": session_id,
                    "session_name": None,
                    "start_time": (
                        session_start.isoformat()
                        if hasattr(session_start, "isoformat")
                        else session_start
                    ),
                    "end_time": (
                        session_end.isoformat()
                        if hasattr(session_end, "isoformat")
                        else session_end
                    ),
                    "duration": float(duration_val) if duration_val else 0,
                    "total_cost": float(_get(row, "total_cost", 4, 0) or 0),
                    "total_tokens": int(_get(row, "total_tokens", 5, 0) or 0),
                    "total_traces_count": int(_get(row, "traces_count", 6, 0) or 0),
                    "first_message": _get(row, "first_message", 7, "") or "",
                    "last_message": _get(row, "last_message", 8, "") or "",
                }
            )
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    # Filter `column_id`s that select a SET of end-user UUIDs against the
    # spans `end_user_id` column. The cross-project user-detail page (and the
    # session view's `user_id` query param) inject one of these as a synthetic
    # `end_user_id IN (...)` filter via `trace_session.py` (it resolves the raw
    # `user_id` string to a list of curated `EndUser.id`s in PG, then passes
    # them here). These must NOT flow into `ClickHouseFilterBuilder.translate()`
    # — they are resolved through the id-remap on a wrapped layer instead (see
    # `_build_resolved_user_clause` / P3b step1.5), so a cross-cutover straddler
    # unifies. `user` is the FilterBuilder alias for `end_user_id`.
    _ENDUSER_ID_FILTER_COLS = frozenset({"end_user_id", "user"})
    _SESSION_ID_FILTER_COLS = SESSION_ID_FILTER_COLS

    def _build_end_user_subquery(self) -> str:
        """Compatibility shim for pre-remap session-list code paths.

        End-user filtering now happens in ``_build_resolved_user_clause`` after
        the span row's ``end_user_id`` has been resolved new->old. Returning an
        empty fragment here prevents duplicate raw predicates.
        """
        return ""

    def _extract_span_filters(self) -> list[dict]:
        """Extract filters that apply at the span level (pre-GROUP BY).

        Filters on aggregate columns (duration, total_cost, etc.) are
        handled separately via HAVING clauses. ``end_user_id``/``user``
        identity filters are ALSO excluded here — they are resolved through
        the id-remap by ``_build_resolved_user_clause`` (P3b step1.5) rather
        than compiled raw by ``ClickHouseFilterBuilder``.
        """
        span_filters: list[dict] = []
        for f in self.filters:
            col_id = f.get("column_id") or f.get("columnId")
            if col_id in self.SESSION_FILTER_MAP or col_id in self.MESSAGE_FILTER_MAP:
                continue
            if (
                col_id in self._ENDUSER_ID_FILTER_COLS
                or col_id in self._SESSION_ID_FILTER_COLS
            ):
                continue
            span_filters.append(f)
        return span_filters

    def _build_resolved_session_clause(self, params: dict[str, Any]) -> str:
        # Applied in the OUTER WHERE of `_session_from_where`, where the column
        # is already projected as the remap-resolved `trace_session_id`.
        return build_session_id_filter_clause(
            self.filters,
            params,
            session_col="trace_session_id",
            param_prefix="sess_",
        )

    def _build_resolved_user_clause(self, params: dict[str, Any]) -> str:
        """Build the id-remap-resolved end-user predicate for the session scan.

        Returns a WHERE-fragment that constrains the (already id-remap-resolved)
        ``end_user_id`` column, or ``""`` when there is no user filter. P3b
        step1.5 (DESIGN §3 / id_remap_sql): the user is selected by the OLD
        curated id(s) — ``self.user_id`` and/or the synthetic ``end_user_id``
        IN-filter both carry ``str(EndUser.id)`` values resolved in PG. A
        cross-cutover straddler's NEW (deterministic-id) spans carry
        ``end_user_id = new_id``; by binding this predicate to the RESOLVED
        (new→old) column produced by the wrapped scan, old + new spans select
        as ONE user. Pre-flip the resolved id == the span's own id, so the
        predicate is identical to the committed bare ``end_user_id = ...`` /
        ``IN (...)`` (gate B). Mutates ``params`` with any bound id values.

        Combines, when both present, ``self.user_id`` (equality) AND every
        extracted ``end_user_id``/``user`` filter (IN / NOT IN) with ``AND`` —
        matching how the committed code would have ``AND``-stitched a
        ``user_clause`` plus a synthetic-filter fragment.
        """
        clauses: list[str] = []

        if self.user_id:
            params["user_id"] = self.user_id
            clauses.append("end_user_id = %(user_id)s")

        eu_param_idx = 0
        for f in self.filters:
            col_id = f.get("column_id") or f.get("columnId")
            if col_id not in self._ENDUSER_ID_FILTER_COLS:
                continue
            config = f.get("filter_config") or f.get("filterConfig") or {}
            filter_op = config.get("filter_op") or config.get("filterOp")
            raw_val = config.get("filter_value", config.get("filterValue"))
            ids = raw_val if isinstance(raw_val, list) else [raw_val]
            ids = [str(v) for v in ids if v]
            if not ids:
                # An empty id-set means "match nothing" — preserve that
                # (the synthetic filter falls back to [NIL_UUID] upstream, but
                # guard here too so we never silently drop the constraint).
                clauses.append("0 = 1")
                continue
            outer_op = "NOT IN" if filter_op in ("not_equals", "not_in") else "IN"
            eu_param_idx += 1
            pname = f"eu_remap_{eu_param_idx}"
            params[pname] = tuple(ids)
            clauses.append(f"end_user_id {outer_op} %({pname})s")

        return " AND ".join(clauses)

    def _user_null_filter_op(self) -> str | None:
        """Return ``is_null``/``is_not_null`` when a user filter tests presence.

        A ``user_id``/``end_user_id`` filter with a null operator carries no
        value to resolve — it asks "does this session have a user at all" —
        and is answered by ``_build_user_presence_clause`` instead of the
        id-set membership in ``_build_resolved_user_clause``.
        """
        for f in self.filters:
            col_id = f.get("column_id") or f.get("columnId")
            if col_id not in self._ENDUSER_ID_FILTER_COLS:
                continue
            config = f.get("filter_config") or f.get("filterConfig") or {}
            op = config.get("filter_op") or config.get("filterOp")
            if op in ("is_null", "is_not_null"):
                return op
        return None

    def _build_user_presence_clause(self, null_op: str) -> str:
        """Membership over sessions that have ANY end user.

        ``is_not_null`` → the session IS in that set; ``is_null`` → it is NOT.
        The outer session query groups by remap-resolved ``trace_session_id``,
        so the presence set must resolve session ids too. Otherwise a straddler
        whose user appears only on its deterministic-id spans can be compared
        against the old survivor id and misclassified as user-less.
        """
        ts_join = remap_left_join(
            "us.trace_session_id", "trace_session_id_remap", "user_presence_ts_remap"
        )
        resolved_ts = resolved_id_expr("us.trace_session_id", "user_presence_ts_remap")
        membership = f"""(
            SELECT trace_session_id
            FROM (
                SELECT {resolved_ts} AS trace_session_id
                FROM (
                    SELECT trace_session_id
                    FROM {self.TABLE}
                    {self.project_where()}
                      AND trace_session_id IS NOT NULL
                      AND trace_session_id != toUUID('{NIL_UUID}')
                      AND end_user_id IS NOT NULL
                      AND end_user_id != toUUID('{NIL_UUID}')
                      AND start_time >= %(start_date)s
                      AND start_time < %(end_date)s
                ) AS us
                {ts_join}
            )
            GROUP BY trace_session_id
        )"""
        op = "NOT IN" if null_op == "is_null" else "IN"
        return f"trace_session_id {op} {membership}"

    def _build_session_user_membership_clause(self, params: dict[str, Any]) -> str:
        null_op = self._user_null_filter_op()
        if null_op:
            return self._build_user_presence_clause(null_op)

        resolved_user_clause = self._build_resolved_user_clause(params)
        if not resolved_user_clause:
            return ""

        ts_join = remap_left_join(
            "us.trace_session_id", "trace_session_id_remap", "user_ts_remap"
        )
        eu_join = remap_left_join(
            "us.end_user_id", "end_user_id_remap", "user_eu_remap"
        )
        resolved_ts = resolved_id_expr("us.trace_session_id", "user_ts_remap")
        resolved_eu = resolved_id_expr("us.end_user_id", "user_eu_remap")
        return f"""trace_session_id IN (
            SELECT trace_session_id
            FROM (
                SELECT
                    {resolved_ts} AS trace_session_id,
                    {resolved_eu} AS end_user_id
                FROM (
                    SELECT trace_session_id, end_user_id
                    FROM {self.TABLE}
                    {self.project_where()}
                      AND trace_session_id IS NOT NULL
                      AND trace_session_id != toUUID('{NIL_UUID}')
                      AND end_user_id IS NOT NULL
                      AND start_time >= %(start_date)s
                      AND start_time < %(end_date)s
                ) AS us
                {ts_join}
                {eu_join}
            )
            WHERE {resolved_user_clause}
            GROUP BY trace_session_id
        )"""

    # Span columns the session aggregates read (kept narrow so the id-remap
    # wrap projects only what the GROUP BY needs).
    _SESSION_SCAN_COLS = (
        "trace_session_id",
        "trace_id",
        "start_time",
        "end_time",
        "cost",
        "total_tokens",
    )

    def _session_from_where(
        self,
        params: dict[str, Any],
        *,
        time_where: str,
        filter_fragment: str,
    ) -> str:
        """Return the ``FROM … WHERE …`` clause for a session aggregation query.

        The scan resolves ``trace_session_id`` new→old before grouping, so a
        cross-cutover straddler remains one session. User filters use a separate
        remap-aware membership subquery; this selects sessions without shrinking
        their aggregate rows. Span predicates remain on the inner root-span scan.

        GATE B: pre-flip every span's id lives in the remap ``old_id`` column, so
        NO span matches a ``new_id`` → ``resolved_id_expr`` (zero-uuid-guarded,
        NOT a COALESCE) returns each span's own id and the LEFT JOIN(s) add
        nothing → the wrapped scan is a transparent pass-through, byte-identical
        (result-set) to the committed bare scan.
        """
        base_predicates = f"""{self.project_where()}
          AND trace_session_id IS NOT NULL
          AND trace_session_id != toUUID('{NIL_UUID}')
          AND (parent_span_id IS NULL OR parent_span_id = '')
          {time_where}
          {filter_fragment}"""

        resolved_session_clause = self._build_resolved_session_clause(params)
        user_membership_clause = self._build_session_user_membership_clause(params)

        # `trace_session_id` resolution is UNCONDITIONAL (closes the browse split);
        # User membership is resolved in a separate session-id subquery so
        # selecting a user does not shrink the session's displayed aggregates.
        ts_join = remap_left_join(
            "rs.trace_session_id", "trace_session_id_remap", "ts_remap"
        )
        resolved_ts = resolved_id_expr("rs.trace_session_id", "ts_remap")

        # Inner scan projects only columns required by session aggregation.
        scan_cols = list(self._SESSION_SCAN_COLS)
        if self._needs_message_aggregates():
            scan_cols.append("input")
        outer_select = [f"{resolved_ts} AS trace_session_id"] + [
            f"rs.{c} AS {c}" for c in scan_cols if c != "trace_session_id"
        ]
        outer_clauses = [
            c for c in (resolved_session_clause, user_membership_clause) if c
        ]

        inner_cols = ", ".join(scan_cols)
        outer_select_sql = ",\n                ".join(outer_select)
        where_clause = (
            f"\n        WHERE {' AND '.join(outer_clauses)}" if outer_clauses else ""
        )
        return f"""FROM (
            SELECT
                {outer_select_sql}
            FROM (
                SELECT {inner_cols}
                FROM {self.TABLE}
                {base_predicates}
            ) AS rs
            {ts_join}
        ){where_clause}"""

    def _build_having_clauses(self) -> str:
        """Build HAVING clause fragments for aggregate-level filters."""
        conditions: list[str] = []
        param_counter = 900  # Use high numbers to avoid conflicts

        for f in self.filters:
            col_id = f.get("column_id") or f.get("columnId")
            if (
                col_id not in self.SESSION_FILTER_MAP
                and col_id not in self.MESSAGE_FILTER_MAP
            ):
                continue

            config = f.get("filter_config") or f.get("filterConfig") or {}
            filter_op = config.get("filter_op") or config.get("filterOp")
            filter_value = config.get("filter_value", config.get("filterValue"))
            ch_col = (
                self.SESSION_FILTER_MAP.get(col_id) or self.MESSAGE_FILTER_MAP[col_id]
            )

            if col_id in self.MESSAGE_FILTER_MAP:
                if filter_op in ("is_null", "is_not_null"):
                    conditions.append(
                        f"({ch_col} IS NULL OR {ch_col} = '')"
                        if filter_op == "is_null"
                        else f"({ch_col} IS NOT NULL AND {ch_col} != '')"
                    )
                    continue
                text_op = {
                    "equals": "=",
                    "not_equals": "!=",
                    "contains": "ILIKE",
                    "not_contains": "NOT ILIKE",
                    "starts_with": "ILIKE",
                    "ends_with": "ILIKE",
                }.get(filter_op)
                if text_op is None:
                    conditions.append("0 = 1")
                    continue
                param_counter += 1
                param_name = f"having_{param_counter}"
                if filter_op in ("contains", "not_contains"):
                    filter_value = f"%{filter_value}%"
                elif filter_op == "starts_with":
                    filter_value = f"{filter_value}%"
                elif filter_op == "ends_with":
                    filter_value = f"%{filter_value}"
                self.params[param_name] = filter_value
                conditions.append(f"{ch_col} {text_op} %({param_name})s")
                continue

            op_map = {
                "equals": "=",
                "not_equals": "!=",
                "greater_than": ">",
                "less_than": "<",
                "greater_than_or_equal": ">=",
                "less_than_or_equal": "<=",
            }
            op = op_map.get(filter_op)
            if op is None:
                conditions.append("0 = 1")
                continue

            param_counter += 1
            param_name = f"having_{param_counter}"
            self.params[param_name] = filter_value
            conditions.append(f"{ch_col} {op} %({param_name})s")

        return " AND ".join(conditions)

    def _has_message_filters(self) -> bool:
        return any(
            (f.get("column_id") or f.get("columnId")) in self.MESSAGE_FILTER_MAP
            for f in self.filters
        )

    def _has_message_sort(self) -> bool:
        return any(
            (s.get("column_id") or s.get("columnId")) in self.MESSAGE_FILTER_MAP
            for s in self.sort_params
        )

    def _needs_message_aggregates(self) -> bool:
        """The argMin/argMax message aggregates must be projected whenever a
        message column is filtered OR sorted on. Sorting alone (without a
        matching filter) still emits ``ORDER BY first_message`` via
        ``translate_sort``, so the column must be selected or CH fails with
        "Unknown expression identifier".
        """
        return self._has_message_filters() or self._has_message_sort()

    def _message_aggregate_select(self) -> str:
        if not self._needs_message_aggregates():
            return ""
        return (
            ",\n            argMin(input, start_time) AS first_message,"
            "\n            argMax(input, start_time) AS last_message"
        )
