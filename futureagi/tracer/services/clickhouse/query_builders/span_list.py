"""
Span List Query Builder for ClickHouse.

Replaces the ``list_spans_observe()`` method in ``tracer.views.observation_span``
with a three-phase ClickHouse query strategy:

Phase 1 -- Paginated spans from the denormalized ``spans`` table (all spans,
not just root spans).

Phase 2 -- Eval scores from ``tracer_eval_logger FINAL`` for the page of
span IDs, grouped by ``(observation_span_id, custom_eval_config_id)``.

Phase 3 -- Annotations from ``model_hub_score FINAL`` for the page of
span IDs, grouped by ``(observation_span_id, label_id)``.

The three result sets are merged in Python to produce the final response.
"""

from datetime import UTC, datetime
from typing import Any

from tracer.services.clickhouse.eval_logger_table import (
    eval_logger_live_state_columns,
    eval_logger_source,
    eval_logger_version_column,
)
from tracer.services.clickhouse.query_builders.base import BaseQueryBuilder
from tracer.services.clickhouse.query_builders.eval_status import (
    non_terminal_eval_marker,
)
from tracer.services.clickhouse.query_builders.filters import ClickHouseFilterBuilder
from tracer.services.clickhouse.query_builders.latest_filter_predicates import (
    partition_span_filter_plans,
    supports_span_filters,
    targets_span_filter_domain,
)
from tracer.services.clickhouse.v2.id_remap_sql import (
    remap_left_join,
    resolved_id_expr,
)


def _unix_microseconds(value: datetime) -> int:
    """Encode DateTime64(6) without driver tuple-datetime precision loss."""

    utc_value = (
        value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    )
    delta = utc_value - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


class SpanListQueryBuilder(BaseQueryBuilder):
    """Build queries for the paginated span list (observe) view.

    Args:
        project_id: Project UUID string.
        page_number: Zero-based page index.
        page_size: Number of spans per page.
        filters: Frontend filter list.
        sort_params: Frontend sort specification list.
        eval_config_ids: List of ``CustomEvalConfig`` UUID strings.
        annotation_label_ids: List of ``AnnotationsLabels`` UUID strings.
    """

    TABLE = "spans"
    ANNOTATION_TABLE = "model_hub_score"
    # Filter compiler class; the v2 list builder overrides this to the v2
    # builder so it reads the v2 dimension tables (end_users, etc.).
    _FILTER_BUILDER_CLS = ClickHouseFilterBuilder

    @staticmethod
    def _normalize_span_entities(
        span_entities: list[tuple[str, str]] | None,
    ) -> tuple[tuple[str, str], ...] | None:
        """Return a finite, de-duplicated set of trace-scoped span IDs.

        ``None`` preserves the legacy bare-ID builder contract for callers that
        have not yet propagated trace identity. An explicitly supplied empty or
        malformed set stays empty and therefore fails closed.
        """

        if span_entities is None:
            return None
        return tuple(
            dict.fromkeys(
                (str(trace_id), str(span_id))
                for trace_id, span_id in span_entities
                if trace_id and span_id
            )
        )

    @staticmethod
    def _normalize_span_identities(
        span_identities: list[tuple[str, str, str, Any]] | None,
    ) -> tuple[tuple[str, str, str, Any], ...] | None:
        """Normalize physical span identities used by the spans table."""

        if span_identities is None:
            return None
        return tuple(
            dict.fromkeys(
                (str(project_id), str(trace_id), str(span_id), start_time)
                for project_id, trace_id, span_id, start_time in span_identities
                if project_id and trace_id and span_id and start_time is not None
            )
        )

    SORT_FIELD_MAP: dict[str, str] = {
        "created_at": "start_time",
        "start_time": "start_time",
        "latency": "latency_ms",
        "latency_ms": "latency_ms",
        "cost": "cost",
        "total_tokens": "total_tokens",
        "name": "name",
        "span_name": "name",
        "status": "status",
    }

    def __init__(
        self,
        project_id: str | None = None,
        project_ids: list[str] | None = None,
        page_number: int = 0,
        page_size: int = 50,
        filters: list[dict] | None = None,
        sort_params: list[dict] | None = None,
        eval_config_ids: list[str] | None = None,
        annotation_label_ids: list[str] | None = None,
        end_user_id: str | None = None,
        project_version_id: str | None = None,
        bounded_internal_scan: bool = False,
        bounded_identity_only: bool = False,
        bounded_sampling_salt: str | None = None,
        bounded_sampling_rate: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(project_id=project_id, project_ids=project_ids, **kwargs)
        self.page_number = page_number
        self.page_size = page_size
        self.filters = filters or []
        self.sort_params = sort_params or []
        self.eval_config_ids = eval_config_ids or []
        self.annotation_label_ids = annotation_label_ids or []
        self.end_user_id = end_user_id
        self.project_version_id = project_version_id
        self._bounded_internal_scan = bool(bounded_internal_scan)
        self._bounded_identity_only = bool(bounded_identity_only)
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

    def supports_bounded_filter_scan(self) -> bool:
        """Whether the latest-state bounded reader can represent this request."""

        try:
            partition_span_filter_plans(self.filters)
        except (TypeError, ValueError):
            return False
        # An unfiltered/time-only custom sort or legacy remapped end-user read
        # is intentionally handled by the finite top-N legacy builder below.
        # The bounded selector has one fixed newest-first order and no remap
        # join, so routing these requests through it would silently change the
        # result even though no attribute filter requires bounded replay.
        if (
            self.sort_params or self.end_user_id
        ) and not self._active_non_time_filters():
            return False
        return (
            supports_span_filters(self.filters)
            and self.bounded_filter_degraded_error_code() is None
        )

    def _active_non_time_filters(self) -> list[dict[str, Any]]:
        return [
            item
            for item in self.filters
            if isinstance(item, dict)
            and (item.get("column_id") or item.get("columnId"))
            not in {"created_at", "start_time"}
        ]

    def bounded_filter_degraded_error_code(self) -> str | None:
        """Explain why a supported filter must not use the broad legacy read."""

        # With no row filter, the top-N legacy path is already project/time
        # bounded and is the only implementation that honours a custom sort or
        # the remapped end-user constraint.  Once a row filter is active those
        # modifiers cannot be combined with the bounded selector's fixed order,
        # so fail closed instead of widening back to a broad filtered scan.
        if not self._active_non_time_filters():
            return None
        if not supports_span_filters(self.filters):
            return (
                "unsupported_filter_shape"
                if targets_span_filter_domain(self.filters)
                else None
            )
        if self.sort_params or self.end_user_id:
            return "unsupported_filter_modifiers"
        return None

    def bounded_filter_row_identity(
        self,
        row: dict[str, Any],
    ) -> tuple[str, str, str, Any]:
        """Return the OTel span identity used by the direct-write table.

        OTel span IDs are unique only inside a trace. Direct-write ``spans``
        rows treat ``(project_id, trace_id, id, start_time)`` as immutable;
        bounded pagination must use that same physical identity instead of
        collapsing unrelated spans that happen to share the 8-byte span ID.
        """

        return (
            str(row.get("project_id") or self.project_id or ""),
            str(row.get("trace_id", "")),
            str(row.get("id", "")),
            row.get("start_time"),
        )

    @staticmethod
    def bounded_filter_row_order_token(
        row: dict[str, Any],
    ) -> tuple[str, str, str]:
        """Stable keyset tiebreak matching the span seed query order."""

        return (
            str(row.get("id", "")),
            str(row.get("trace_id", "")),
            str(row.get("project_id", "")),
        )

    @staticmethod
    def recommended_filter_seed_batch_size() -> int:
        return 200

    @staticmethod
    def recommended_filter_classify_batch_size() -> int:
        return 200

    def build_filter_seed_page(
        self,
        *,
        slice_start: Any,
        slice_end: Any,
        limit: int,
        before_start_time: Any = None,
        before_id: Any = None,
    ) -> tuple[str, dict[str, Any]]:
        """Return bounded physical matches as a latest-state candidate superset."""

        if not self.supports_bounded_filter_scan():
            raise ValueError("unsupported bounded span filter scan")
        if limit <= 0:
            raise ValueError("limit must be greater than zero")
        if (before_start_time is None) != (before_id is None):
            raise ValueError("span keyset values must be provided together")
        request_start, request_end = self.parse_time_range(self.filters)
        if not request_start <= slice_start < slice_end <= request_end:
            raise ValueError("span seed slice must stay inside the request window")
        self.params.update({"start_date": request_start, "end_date": request_end})

        plans, _ = partition_span_filter_plans(self.filters)
        params: dict[str, Any] = {
            **self.params,
            "filter_slice_start": slice_start,
            "filter_slice_end": slice_end,
            "filter_seed_limit": int(limit),
        }
        project_version_fragment = ""
        if self.project_version_id:
            params["project_version_id"] = self.project_version_id
            project_version_fragment = "AND project_version_id = %(project_version_id)s"
        for plan in plans:
            params.update(
                {
                    key: value
                    for key, value in plan.params.items()
                    if f"%({key})s" in plan.seed_predicate
                }
            )
        predicate = " AND ".join(plan.seed_predicate for plan in plans) or "1 = 1"
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="span_seed_time_exclusion",
            )
        )
        params.update(datetime_params)
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )

        sampling_fragment = ""
        if self._bounded_sampling_rate is not None:
            params["bounded_sampling_salt"] = str(self._bounded_sampling_salt)
            params["bounded_sampling_rate"] = float(self._bounded_sampling_rate)
            sampling_fragment = """
              AND modulo(
                  cityHash64(
                      %(bounded_sampling_salt)s,
                      toString(project_id),
                      toString(trace_id),
                      toString(id)
                  ), 100
              ) < %(bounded_sampling_rate)s
            """

        keyset_fragment = ""
        if before_start_time is not None:
            if not slice_start <= before_start_time < slice_end:
                raise ValueError("span keyset must stay inside its slice")
            if not (
                isinstance(before_id, tuple)
                and len(before_id) == 3
                and all(isinstance(value, str) for value in before_id)
            ):
                raise ValueError(
                    "span keyset identity must be an (id, trace_id, project_id) tuple"
                )
            params["filter_before_start_us"] = _unix_microseconds(before_start_time)
            params["filter_before_id"] = before_id[0]
            params["filter_before_trace_id"] = before_id[1]
            params["filter_before_project_id"] = before_id[2]
            keyset_fragment = """
              AND (
                  toUnixTimestamp64Micro(start_time) < %(filter_before_start_us)s
                  OR (
                      toUnixTimestamp64Micro(start_time) = %(filter_before_start_us)s
                      AND (
                          id < %(filter_before_id)s
                          OR (
                              id = %(filter_before_id)s
                              AND (
                                  trace_id < %(filter_before_trace_id)s
                                  OR (
                                      trace_id = %(filter_before_trace_id)s
                                      AND project_id < toUUID(%(filter_before_project_id)s)
                                  )
                              )
                          )
                      )
                  )
              )
            """

        query = f"""
        SELECT project_id, id, trace_id, start_time
        FROM {self.TABLE}
        PREWHERE {self.project_filter_sql()}
          AND is_deleted = 0
          {project_version_fragment}
          AND start_time >= %(filter_slice_start)s
          AND start_time < %(filter_slice_end)s
        WHERE {predicate}{datetime_fragment}
          {sampling_fragment}
          {keyset_fragment}
        ORDER BY start_time DESC, id DESC, trace_id DESC, project_id DESC
        LIMIT 1 BY project_id, trace_id, id, start_time
        LIMIT %(filter_seed_limit)s
        """
        return query, params

    def build_filter_match_query(
        self,
        candidate_ids: list[str],
        *,
        candidate_full_state: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        """Hydrate exact matching spans after resolving every latest version."""

        span_ids = tuple(dict.fromkeys(str(value) for value in candidate_ids if value))
        return self._build_filter_match_query(
            span_ids=span_ids,
            candidate_identities=None,
            candidate_full_state=candidate_full_state,
        )

    def build_filter_match_query_from_seed_rows(
        self,
        candidate_rows: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any]]:
        """Classify only the trace-scoped span identities returned by the seed."""

        normalized_rows: list[dict[str, Any]] = []
        for row in candidate_rows:
            if row.get("project_id"):
                normalized_rows.append(row)
            elif self.project_id:
                # Compatibility for single-project executors/tests that project
                # the pre-hardening seed shape. Org-scoped reads must carry the
                # project explicitly and therefore cannot take this fallback.
                normalized_rows.append({**row, "project_id": self.project_id})

        identities: tuple[tuple[str, str, str, Any], ...] = tuple(
            dict.fromkeys(
                self.bounded_filter_row_identity(row)
                for row in normalized_rows
                if row.get("project_id") and row.get("id") and row.get("trace_id")
            )
        )
        span_ids = tuple(dict.fromkeys(identity[2] for identity in identities))
        return self._build_filter_match_query(
            span_ids=span_ids,
            candidate_identities=identities,
            candidate_full_state=False,
        )

    def _build_filter_match_query(
        self,
        *,
        span_ids: tuple[str, ...],
        candidate_identities: tuple[tuple[str, str, str, Any], ...] | None,
        candidate_full_state: bool,
    ) -> tuple[str, dict[str, Any]]:
        """Resolve candidates by full physical span identity and latest state."""

        if not span_ids:
            return "", {}
        candidate_count = len(candidate_identities or span_ids)
        if candidate_count > 200:
            raise ValueError("candidate span batch exceeds bounded limit")
        if not self.supports_bounded_filter_scan():
            raise ValueError("unsupported bounded span filter scan")

        request_start, request_end = self.parse_time_range(self.filters)
        self.params.update({"start_date": request_start, "end_date": request_end})
        has_explicit_time_filter = any(
            (item.get("column_id") or item.get("columnId"))
            in {"created_at", "start_time"}
            for item in self.filters
        )
        scope_to_request_window = not candidate_full_state or has_explicit_time_filter
        plans, residual_filters = partition_span_filter_plans(self.filters)
        params: dict[str, Any] = {
            **self.params,
            "candidate_span_ids": span_ids,
        }
        if scope_to_request_window:
            params.update(
                {
                    "candidate_start_date": request_start,
                    "candidate_end_date": request_end,
                }
            )
        project_version_fragment = ""
        if self.project_version_id:
            params["project_version_id"] = self.project_version_id
            project_version_fragment = "AND project_version_id = %(project_version_id)s"
        candidate_scope_fragment = ""
        candidate_entities_param = None
        if candidate_identities is not None:
            params["candidate_span_trace_ids"] = tuple(
                dict.fromkeys(identity[1] for identity in candidate_identities)
            )
            params["candidate_span_project_ids"] = tuple(
                dict.fromkeys(identity[0] for identity in candidate_identities)
            )
            params["candidate_span_dates"] = tuple(
                dict.fromkeys(identity[3].date() for identity in candidate_identities)
            )
            params["candidate_span_identities"] = tuple(
                (
                    identity[0],
                    identity[1],
                    identity[2],
                    _unix_microseconds(identity[3]),
                )
                for identity in candidate_identities
            )
            params["candidate_span_entities"] = tuple(
                dict.fromkeys(
                    (identity[1], identity[2]) for identity in candidate_identities
                )
            )
            candidate_entities_param = "candidate_span_entities"
            candidate_scope_fragment = """
                  AND trace_id IN %(candidate_span_trace_ids)s
                  AND project_id IN %(candidate_span_project_ids)s
                WHERE toDate(start_time) IN %(candidate_span_dates)s
                  AND (
                      toString(project_id), trace_id, id,
                      toUnixTimestamp64Micro(start_time)
                  )
                      IN %(candidate_span_identities)s
            """
        for plan in plans:
            params.update(plan.params)
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="latest_start_time",
                param_prefix="span_match_time_exclusion",
            )
        )
        params.update(datetime_params)
        datetime_fragment = (
            f"\n              AND {datetime_predicate}" if datetime_predicate else ""
        )
        aggregates = [aggregate for plan in plans for aggregate in plan.aggregates]
        aggregate_fragment = (
            ",\n                " + ",\n                ".join(aggregates)
            if aggregates
            else ""
        )
        predicate = " AND ".join(plan.predicate for plan in plans) or "1 = 1"

        residual_predicate = "1 = 1"
        if residual_filters:
            residual_builder = self._FILTER_BUILDER_CLS(
                table=self.TABLE,
                query_mode=self._FILTER_BUILDER_CLS.QUERY_MODE_SPAN,
                annotation_label_ids=self.annotation_label_ids,
                project_id=self.project_id,
                project_ids=self.project_ids,
                score_date_scope=scope_to_request_window,
                span_date_scope=scope_to_request_window,
                candidate_ids_param="candidate_span_ids",
                candidate_entities_param=candidate_entities_param,
            )
            residual_predicate, residual_params = residual_builder.translate(
                residual_filters
            )
            params.update(residual_params)
            residual_predicate = residual_predicate or "1 = 1"

        if self._bounded_identity_only:
            select_fragment = (
                "grouped_project_id AS project_id, grouped_id AS id, "
                "latest_trace_id AS trace_id, "
                "latest_start_time AS start_time"
            )
            hydrate_aggregate_fragment = ",\n                argMax(trace_id, _peerdb_version) AS latest_trace_id"
        else:
            select_fragment = """grouped_project_id AS project_id,
            grouped_id AS id,
            latest_trace_id AS trace_id,
            latest_name AS name,
            latest_observation_type AS observation_type,
            latest_status AS status,
            latest_start_time AS start_time,
            latest_end_time AS end_time,
            latest_latency_ms AS latency_ms,
            latest_cost AS cost,
            latest_total_tokens AS total_tokens,
            latest_prompt_tokens AS prompt_tokens,
            latest_completion_tokens AS completion_tokens,
            latest_model AS model,
            latest_provider AS provider,
            latest_end_user_id AS end_user_id,
            latest_created_at AS created_at"""
            hydrate_aggregate_fragment = """,
                argMax(trace_id, _peerdb_version) AS latest_trace_id,
                argMax(name, _peerdb_version) AS latest_name,
                argMax(observation_type, _peerdb_version)
                    AS latest_observation_type,
                argMax(tuple(status), _peerdb_version).1 AS latest_status,
                argMax(tuple(end_time), _peerdb_version).1 AS latest_end_time,
                argMax(tuple(latency_ms), _peerdb_version).1 AS latest_latency_ms,
                argMax(tuple(cost), _peerdb_version).1 AS latest_cost,
                argMax(tuple(total_tokens), _peerdb_version).1
                    AS latest_total_tokens,
                argMax(tuple(prompt_tokens), _peerdb_version).1
                    AS latest_prompt_tokens,
                argMax(tuple(completion_tokens), _peerdb_version).1
                    AS latest_completion_tokens,
                argMax(tuple(model), _peerdb_version).1 AS latest_model,
                argMax(tuple(provider), _peerdb_version).1 AS latest_provider,
                argMax(tuple(end_user_id), _peerdb_version).1 AS latest_end_user_id,
                argMax(created_at, _peerdb_version) AS latest_created_at"""

        latest_time_fragment = (
            "\n              AND latest_start_time >= %(candidate_start_date)s"
            "\n              AND latest_start_time < %(candidate_end_date)s"
            if scope_to_request_window
            else ""
        )
        query = f"""
        SELECT *
        FROM (
            SELECT
                {select_fragment}
            FROM (
                SELECT
                    project_id AS grouped_project_id,
                    id AS grouped_id,
                    argMax(start_time, _peerdb_version) AS latest_start_time,
                    argMax(is_deleted, _peerdb_version) AS latest_is_deleted
                    {hydrate_aggregate_fragment}
                    {aggregate_fragment}
                FROM {self.TABLE}
                PREWHERE {self.project_filter_sql()}
                  {project_version_fragment}
                  AND id IN %(candidate_span_ids)s
                  {candidate_scope_fragment}
                GROUP BY project_id, trace_id, id, start_time
            )
            WHERE latest_is_deleted = 0{latest_time_fragment}{datetime_fragment}
              AND {predicate}
        ) AS latest_candidates
        WHERE {residual_predicate}
        ORDER BY start_time DESC, id DESC, trace_id DESC, project_id DESC
        LIMIT {candidate_count}
        """
        return query, params

    # ------------------------------------------------------------------
    # Phase 1: Paginated span list
    # ------------------------------------------------------------------

    def build(self, since: Any = None) -> tuple[str, dict[str, Any]]:
        """Build the Phase-1 query for paginated span data.

        Args:
            since: Optional narrowed window start (datetime) for progressive
                time-slice pagination. When set, an additional
                ``start_time >= %(slice_start)s`` predicate restricts the scan
                to the newest slice — because the sort is ``start_time DESC``,
                every row in a newer slice sorts before every older row, so a
                slice that yields the full prefix IS the global prefix and the
                caller can stop without scanning the whole window. The regular
                ``start_date``/``end_date`` params are left untouched so
            ``build_count_query()`` still counts the full window.
        """
        if error_code := self.bounded_filter_degraded_error_code():
            raise ValueError(f"unsafe legacy filtered span read blocked: {error_code}")
        start_date, end_date = self.parse_time_range(self.filters)
        self.params["start_date"] = start_date
        self.params["end_date"] = end_date

        fb = self._FILTER_BUILDER_CLS(
            table=self.TABLE,
            query_mode=self._FILTER_BUILDER_CLS.QUERY_MODE_SPAN,
            annotation_label_ids=self.annotation_label_ids,
            project_id=self.project_id,
            project_ids=self.project_ids,
        )
        extra_where, extra_params = fb.translate(self.filters)
        self.params.update(extra_params)
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="span_list_time_exclusion",
            )
        )
        self.params.update(datetime_params)

        order_clause = fb.translate_sort(
            self.sort_params, field_map=self.SORT_FIELD_MAP
        )
        if not order_clause:
            # `id DESC` tiebreak: ClickHouse's parallel sort is not stable, so
            # equal start_time rows can permute between requests. Prefix-dedup
            # pagination (page_dedup.py) slices consecutive pages out of what
            # must be ONE stable global order — without the tiebreak a row can
            # appear on two pages or be skipped. Deterministic order also makes
            # the progressive-slice prefix (see `since`) reproducible.
            order_clause = (
                "ORDER BY start_time DESC, id DESC, trace_id DESC, project_id DESC"
            )

        slice_fragment = ""
        if since is not None:
            self.params["slice_start"] = since
            slice_fragment = "AND start_time >= %(slice_start)s"

        # Prefix-fetch pagination: read the sorted prefix [0, offset +
        # 2*page_size) in ONE bounded top-K pass and let the view dedup by
        # span id then slice [offset, offset + page_size) — see
        # tracer/services/clickhouse/page_dedup.py. This preserves the
        # global-dedup semantics `LIMIT 1 BY id` provided (a key can never
        # appear on two pages) without its O(window) full sort; the 2x
        # page_size margin keeps pages exact for up to page_size duplicate
        # rows in the prefix. No SQL OFFSET — slicing happens in Python.
        offset = self.page_number * self.page_size
        self.params["limit"] = offset + 2 * self.page_size

        filter_fragment = f"AND {extra_where}" if extra_where else ""
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )

        end_user_fragment = ""
        if self.end_user_id:
            end_user_fragment = "AND end_user_id = %(end_user_id)s"
            self.params["end_user_id"] = self.end_user_id

        pv_fragment = ""
        if self.project_version_id:
            pv_fragment = "AND project_version_id = %(project_version_id)s"
            self.params["project_version_id"] = self.project_version_id

        # P3b step1.5 id-remap resolution (DESIGN §3 / id_remap_sql): this is the
        # per-user span list — `end_user_id` is passed as the OLD curated id
        # (obs_span view resolves `user_id` → `EndUser.objects.get(...).id`). A
        # cross-cutover straddler's NEW (deterministic-id) spans carry
        # `end_user_id = new_id`, so resolve each span new→old through
        # `end_user_id_remap` BEFORE the user filter, and re-project the resolved
        # id AS `end_user_id` so the displayed column also reads under the OLD
        # identity. The non-user predicates (project / time / version / generic
        # `{filter_fragment}`) stay on the bare `{self.TABLE}` inner scan (they
        # may reference span columns this wrap does not project); only the
        # identity resolve+filter moves to the wrapped layer. `resolved_id_expr`
        # is the zero-uuid-guarded new→old map — NOT a COALESCE; an unmatched
        # LEFT JOIN fills `old_id` with the zero-uuid, not NULL (see id_remap_sql).
        # Gated on `self.end_user_id`: a non-user span list keeps the committed
        # bare-`spans` query verbatim (out of scope). Pre-flip even the user path
        # is byte-identical — NO span matches a `new_id`, so the resolved id ==
        # the span's own id (gate B).
        if self.end_user_id:
            remap_join = remap_left_join("rs.end_user_id", "end_user_id_remap")
            resolved_eu = resolved_id_expr("rs.end_user_id")
            inner_scan = f"""
            SELECT
                project_id,
                id,
                trace_id,
                name,
                observation_type,
                status,
                start_time,
                end_time,
                latency_ms,
                cost,
                total_tokens,
                prompt_tokens,
                completion_tokens,
                model,
                provider,
                end_user_id,
                created_at
            FROM {self.TABLE}
            {self.project_where()}
              AND created_at >= %(start_date)s - INTERVAL 1 DAY
              AND start_time >= %(start_date)s
              AND start_time < %(end_date)s
              {slice_fragment}
              {pv_fragment}
              {filter_fragment}
            """
            query = f"""
            SELECT
                project_id,
                id,
                trace_id,
                name,
                observation_type,
                status,
                start_time,
                end_time,
                latency_ms,
                cost,
                total_tokens,
                prompt_tokens,
                completion_tokens,
                model,
                provider,
                resolved_end_user_id AS end_user_id,
                created_at
            FROM (
                SELECT
                    rs.*,
                    {resolved_eu} AS resolved_end_user_id
                FROM ({inner_scan}) AS rs
                {remap_join}
            )
            WHERE resolved_end_user_id = %(end_user_id)s
            {order_clause}
            LIMIT %(limit)s
            """
            return query, self.params

        # Light columns only — input/output fetched via build_content_query().
        #
        # PERF: no `LIMIT 1 BY id`. On a wide time window that clause forced CH
        # to read + full-sort EVERY matching row (the whole window) to dedup by
        # id before applying ORDER BY … LIMIT — O(rows-in-window) memory that
        # OOM-crashed the server at ~10M+ rows. Dropping it lets `ORDER BY
        # start_time DESC LIMIT n` run as a bounded top-N (a size-n priority
        # queue, O(n) memory), so the page returns without materializing the
        # window. ReplacingMergeTree duplicate span versions are rare + transient
        # (collapsed on the next merge); the view dedups the returned page by
        # span_id in Python to keep one row per span. `is_deleted = 0` (from
        # project_where) still excludes soft-deleted rows.
        query = f"""
        SELECT
            project_id,
            id,
            trace_id,
            name,
            observation_type,
            status,
            start_time,
            end_time,
            latency_ms,
            cost,
            total_tokens,
            prompt_tokens,
            completion_tokens,
            model,
            provider,
            end_user_id,
            created_at
        FROM {self.TABLE}
        {self.project_where()}
          AND created_at >= %(start_date)s - INTERVAL 1 DAY
          AND start_time >= %(start_date)s
          AND start_time < %(end_date)s{datetime_fragment}
          {slice_fragment}
          {end_user_fragment}
          {pv_fragment}
          {filter_fragment}
        {order_clause}
        LIMIT %(limit)s
        """
        return query, self.params

    def build_id_query(
        self,
        *,
        limit: int | None = None,
        created_at_floor: datetime | None = None,
        created_at_ceiling: datetime | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Filtered span ids only — same filter/time window as build(), no pivots.

        With ``limit=None`` (default) the full matching id set is returned
        unordered, which is what the eval resolver wants. Pass ``limit`` for a
        capped "select all matching" resolve: the ids are then ordered
        newest-first (``start_time DESC, id DESC`` — same as the observe list and
        the PG bulk-select) so the cap keeps the most recent rows deterministically.

        ``created_at_floor`` (continuous eval tasks only): floor the scan on CH
        arrival time (``created_at``) instead of event time (``start_time``), so a
        span whose ingestion lagged its ``start_time`` is still picked up. ``None``
        keeps the ``start_time`` window used by the UI list and historical tasks.
        """
        start_date, end_date = self.parse_time_range(self.filters)
        if created_at_floor is not None:
            self.params["created_at_floor"] = created_at_floor
            time_where = "AND created_at >= %(created_at_floor)s"
            if created_at_ceiling is not None:
                self.params["created_at_ceiling"] = created_at_ceiling
                time_where += " AND created_at < %(created_at_ceiling)s"
        else:
            time_where = (
                "AND created_at >= %(start_date)s - INTERVAL 1 DAY "
                "AND start_time >= %(start_date)s "
                "AND start_time < %(end_date)s"
            )
        self.params["start_date"] = start_date
        self.params["end_date"] = end_date

        fb = self._FILTER_BUILDER_CLS(
            table=self.TABLE,
            query_mode=self._FILTER_BUILDER_CLS.QUERY_MODE_SPAN,
            annotation_label_ids=self.annotation_label_ids,
            project_id=self.project_id,
            project_ids=self.project_ids,
        )
        extra_where, extra_params = fb.translate(self.filters)
        self.params.update(extra_params)
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="span_id_time_exclusion",
            )
        )
        self.params.update(datetime_params)
        filter_fragment = f"AND {extra_where}" if extra_where else ""
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )

        pv_fragment = ""
        if self.project_version_id:
            pv_fragment = "AND project_version_id = %(project_version_id)s"
            self.params["project_version_id"] = self.project_version_id

        order_fragment = ""
        limit_fragment = ""
        if limit is not None:
            order_fragment = "ORDER BY start_time DESC, id DESC"
            limit_fragment = "LIMIT %(id_limit)s"
            self.params["id_limit"] = int(limit)

        query = f"""
        SELECT id
        FROM {self.TABLE}
        {self.project_where()}
          {time_where}{datetime_fragment}
          {pv_fragment}
          {filter_fragment}
        {order_fragment}
        LIMIT 1 BY id
        {limit_fragment}
        """
        return query, self.params

    def build_content_query(
        self,
        span_ids: list[str],
        *,
        span_identities: list[tuple[str, str, str, Any]] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Fetch latest content for exact physical spans on the returned page."""
        identities = self._normalize_span_identities(span_identities)
        if identities is not None:
            span_ids = list(dict.fromkeys(span_id for _, _, span_id, _ in identities))
        if not span_ids or identities == ():
            return "", {}
        if "start_date" not in self.params or "end_date" not in self.params:
            start_date, end_date = self.parse_time_range(self.filters)
            self.params["start_date"] = start_date
            self.params["end_date"] = end_date
        params = {**self.params, "content_span_ids": tuple(span_ids)}
        trace_prewhere = ""
        project_prewhere = ""
        entity_where = ""
        if identities is not None:
            params["content_trace_ids"] = tuple(
                dict.fromkeys(trace_id for _, trace_id, _, _ in identities)
            )
            params["content_project_ids"] = tuple(
                dict.fromkeys(project_id for project_id, _, _, _ in identities)
            )
            params["content_span_dates"] = tuple(
                dict.fromkeys(start_time.date() for _, _, _, start_time in identities)
            )
            params["content_span_identities"] = tuple(
                (
                    project_id,
                    trace_id,
                    span_id,
                    _unix_microseconds(start_time),
                )
                for project_id, trace_id, span_id, start_time in identities
            )
            trace_prewhere = "AND trace_id IN %(content_trace_ids)s"
            project_prewhere = "AND project_id IN %(content_project_ids)s"
            entity_where = (
                "AND toDate(start_time) IN %(content_span_dates)s "
                "AND (toString(project_id), trace_id, id, "
                "toUnixTimestamp64Micro(start_time)) "
                "IN %(content_span_identities)s"
            )
        query = f"""
        SELECT project_id, trace_id, id, start_time, input, output,
               attributes_extra,
               attrs_string, attrs_number, attrs_bool
        FROM (
            SELECT project_id, trace_id, id, start_time, input, output,
                   attributes_extra,
                   span_attr_str AS attrs_string,
                   span_attr_num AS attrs_number,
                   span_attr_bool AS attrs_bool,
                   _peerdb_is_deleted AS latest_is_deleted
            FROM {self.TABLE}
            PREWHERE id IN %(content_span_ids)s
              {trace_prewhere}
              {project_prewhere}
            WHERE {self.project_filter_sql()}
              {entity_where}
              AND start_time >= %(start_date)s - INTERVAL 1 DAY
              AND start_time < %(end_date)s + INTERVAL 1 DAY
            ORDER BY _peerdb_version DESC
            LIMIT 1 BY project_id, trace_id, id, start_time
        )
        WHERE latest_is_deleted = 0
        """
        return query, params

    def build_count_query(self) -> tuple[str, dict[str, Any]]:
        """Build a count query for total matching spans.

        PERF: uses ``count()`` rather than ``uniqExact(id)``. ``uniqExact`` built
        an exact hash set of every matching span id (tens of millions of 16-char
        strings) — hundreds of MB to GBs of unbounded memory that OOM-crashed the
        server on large windows. ``count()`` reads only the filter columns and
        needs O(1) memory. The pagination total is a display value; the only
        difference is that a transient un-merged ReplacingMergeTree duplicate is
        counted once extra, which is immaterial and self-heals on the next merge
        (and matches the list, which no longer de-dups via ``LIMIT 1 BY id``).
        """
        if error_code := self.bounded_filter_degraded_error_code():
            raise ValueError(f"unsafe legacy filtered span count blocked: {error_code}")
        fb = self._FILTER_BUILDER_CLS(
            table=self.TABLE,
            query_mode=self._FILTER_BUILDER_CLS.QUERY_MODE_SPAN,
            annotation_label_ids=self.annotation_label_ids,
            project_id=self.project_id,
            project_ids=self.project_ids,
        )
        extra_where, extra_params = fb.translate(self.filters)
        params = dict(self.params)
        params.update(extra_params)
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="span_count_time_exclusion",
            )
        )
        params.update(datetime_params)

        filter_fragment = f"AND {extra_where}" if extra_where else ""
        datetime_fragment = (
            f"\n                      AND {datetime_predicate}"
            if datetime_predicate
            else ""
        )
        datetime_outer_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )

        end_user_fragment = ""
        if self.end_user_id:
            end_user_fragment = "AND end_user_id = %(end_user_id)s"
            params["end_user_id"] = self.end_user_id

        pv_fragment = ""
        if self.project_version_id:
            pv_fragment = "AND project_version_id = %(project_version_id)s"
            params["project_version_id"] = self.project_version_id

        # P3b step1.5 id-remap resolution (DESIGN §3 / id_remap_sql): MUST mirror
        # `build()` exactly — resolve `end_user_id` new→old and count on the
        # resolved id, else a straddler's count splits from the list and
        # has_more/pagination lies. Non-user predicates stay on the bare inner
        # scan; pre-flip a byte-identical no-op (gate B). Gated on
        # `self.end_user_id` like `build()`.
        if self.end_user_id:
            remap_join = remap_left_join("rs.end_user_id", "end_user_id_remap")
            resolved_eu = resolved_id_expr("rs.end_user_id")
            query = f"""
            SELECT count() AS total
            FROM (
                SELECT rs.id AS id, {resolved_eu} AS resolved_end_user_id
                FROM (
                    SELECT id, end_user_id
                    FROM {self.TABLE}
                    {self.project_where()}
                      AND created_at >= %(start_date)s - INTERVAL 1 DAY
                      AND start_time >= %(start_date)s
                      AND start_time < %(end_date)s{datetime_fragment}
                      {pv_fragment}
                      {filter_fragment}
                ) AS rs
                {remap_join}
            )
            WHERE resolved_end_user_id = %(end_user_id)s
            """
            return query, params

        query = f"""
        SELECT count() AS total
        FROM {self.TABLE}
        {self.project_where()}
          AND created_at >= %(start_date)s - INTERVAL 1 DAY
          AND start_time >= %(start_date)s
          AND start_time < %(end_date)s{datetime_outer_fragment}
          {end_user_fragment}
          {pv_fragment}
          {filter_fragment}
        """
        return query, params

    # ------------------------------------------------------------------
    # Phase 2: Eval scores for a set of span IDs
    # ------------------------------------------------------------------

    def build_eval_query(
        self,
        span_ids: list[str],
        created_after: Any = None,
        *,
        span_entities: list[tuple[str, str]] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Build the Phase-2 eval-scores query for a page of span IDs.

        Args:
            created_after: Optional datetime lower bound for partition pruning.
                The eval table is ``PARTITION BY toYYYYMM(created_at)``; without
                a ``created_at`` predicate the span-id probe touches EVERY
                monthly partition. An eval row cannot be created before its span
                row exists, so bounding by the page's oldest ``created_at``
                (minus a 7-day safety margin) prunes to the relevant partitions
                — measured 55x fewer rows read at 10M eval rows.
        """
        entities = self._normalize_span_entities(span_entities)
        if entities is not None:
            span_ids = list(dict.fromkeys(span_id for _, span_id in entities))
        if not span_ids or not self.eval_config_ids or entities == ():
            return "", {}

        params: dict[str, Any] = {
            "span_ids": tuple(span_ids),
            "eval_config_ids": tuple(self.eval_config_ids),
        }
        created_fragment = ""
        if created_after is not None:
            params["evals_created_after"] = created_after
            created_fragment = (
                "AND created_at >= %(evals_created_after)s - INTERVAL 7 DAY"
            )

        trace_select = ""
        trace_inner_select = ""
        trace_group = ""
        entity_fragment = ""
        if entities is not None:
            params["eval_trace_ids"] = tuple(
                dict.fromkeys(trace_id for trace_id, _ in entities)
            )
            params["eval_span_entities"] = entities
            trace_select = "toString(trace_id) AS trace_id,"
            trace_inner_select = "trace_id,"
            trace_group = "trace_id,"
            entity_fragment = """
              AND NOT isNull(trace_id)
              AND toString(trace_id) IN %(eval_trace_ids)s
              AND (toString(trace_id), observation_span_id) IN %(eval_span_entities)s
            """

        eval_table, _ = eval_logger_source(include_cdc_tombstone_guard=True)
        eval_version_col = eval_logger_version_column(eval_table)
        live_columns = eval_logger_live_state_columns(eval_table)
        live_projection = ",\n                ".join(
            f"{column} AS latest_state_{index}"
            for index, column in enumerate(live_columns)
        )
        live_predicate = " AND ".join(
            (
                f"latest_state_{index} = 0"
                if column != "deleted"
                else f"(latest_state_{index} = 0 OR latest_state_{index} IS NULL)"
            )
            for index, column in enumerate(live_columns)
        )

        # Aggregates are computed only over *completed*, non-errored rows so a
        # non-terminal (pending/running) or skipped row never skews a score or
        # masquerades as a real value. The per-status counts let the pivot pick
        # one cell state per (span, config) by the precedence
        # completed > errored > skipped > running > pending.
        # ``success_count`` excludes the non-terminal / skipped / errored
        # states via ``status NOT IN (...)``: a bare ``error = 0`` guard also
        # matches pending/running/skipped rows (they carry ``error = 0`` and a
        # NULL output), which would collapse the pivot's "is there a real
        # score?" test. A NOT-IN (rather than ``status = 'completed'``) keeps
        # legacy rows whose mirrored ``status`` is empty/NULL counted as
        # completed, so historical scores don't blank out.
        # ``str_lists`` keeps every completed ``output_str_list`` so the pivot
        # can compute per-choice percentages for CHOICES evals (column shape:
        # ``{config_id}**{choice}``).
        # ``output_str`` is Nullable(String); ClickHouse 3-valued logic makes
        # ``NULL != 'ERROR'`` NULL (not TRUE), so use ``ifNull(...)`` to keep
        # the comparison NULL-safe.
        query = f"""
        SELECT
            {trace_select}
            observation_span_id,
            toString(custom_eval_config_id) AS eval_config_id,
            -- ifNotFinite(, NULL): avgIf over an all-NULL group returns NaN, which
            -- json.dumps(allow_nan=False) rejects. NULL serializes as null.
            ifNotFinite(avgIf(
                output_float,
                error = 0 AND ifNull(output_str, '') != 'ERROR' AND status NOT IN ('pending', 'running', 'skipped', 'errored')
            ), NULL) AS avg_score,
            ifNotFinite(avgIf(
                CASE WHEN output_bool = 1 THEN 100.0 ELSE 0.0 END,
                error = 0 AND ifNull(output_str, '') != 'ERROR' AND status NOT IN ('pending', 'running', 'skipped', 'errored')
            ), NULL) AS pass_rate,
            countIf(
                error = 0 AND ifNull(output_str, '') != 'ERROR' AND status NOT IN ('pending', 'running', 'skipped', 'errored')
            ) AS success_count,
            countIf(
                error = 1 OR ifNull(output_str, '') = 'ERROR' OR status = 'errored'
            ) AS error_count,
            countIf(status = 'skipped') AS skipped_count,
            countIf(status = 'running') AS running_count,
            countIf(status = 'pending') AS pending_count,
            anyIf(skipped_reason, status = 'skipped') AS skipped_reason,
            count() AS eval_count,
            groupArrayIf(
                output_str_list,
                error = 0 AND ifNull(output_str, '') != 'ERROR' AND status NOT IN ('pending', 'running', 'skipped', 'errored')
            ) AS str_lists
        -- Candidate-scoped latest replay. Live/tombstone predicates are
        -- intentionally outside LIMIT 1 BY id so a newest deletion marker
        -- cannot resurrect an older score.
        FROM (
            SELECT
                {trace_inner_select}
                observation_span_id,
                custom_eval_config_id,
                output_float,
                output_bool,
                output_str,
                output_str_list,
                error,
                status,
                skipped_reason,
                {live_projection}
            FROM {eval_table}
            WHERE observation_span_id IN %(span_ids)s
              {entity_fragment}
              AND custom_eval_config_id IN %(eval_config_ids)s
              {created_fragment}
            ORDER BY {eval_version_col} DESC
            LIMIT 1 BY id
        )
        WHERE {live_predicate}
        GROUP BY {trace_group}observation_span_id, custom_eval_config_id
        SETTINGS max_bytes_before_external_group_by = 1073741824, max_bytes_before_external_sort = 1073741824
        """
        return query, params

    # ------------------------------------------------------------------
    # Phase 3: Annotations for a set of span IDs
    # ------------------------------------------------------------------

    def build_annotation_query(
        self,
        span_ids: list[str],
        created_after: Any = None,
        *,
        span_entities: list[tuple[str, str]] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Build the Phase-3 annotation query for a page of span IDs.

        Args:
            created_after: Optional datetime lower bound — same partition-prune
                rationale as ``build_eval_query`` (``model_hub_score`` is also
                ``PARTITION BY toYYYYMM(created_at)``; a score row cannot
                pre-date its span row).
        """
        entities = self._normalize_span_entities(span_entities)
        if entities is not None:
            span_ids = list(dict.fromkeys(span_id for _, span_id in entities))
        if not span_ids or not self.annotation_label_ids or entities == ():
            return "", {}

        params: dict[str, Any] = {
            "span_ids": tuple(span_ids),
            "label_ids": tuple(self.annotation_label_ids),
        }
        created_fragment = ""
        if created_after is not None:
            params["anns_created_after"] = created_after
            created_fragment = (
                "AND created_at >= %(anns_created_after)s - INTERVAL 7 DAY"
            )

        trace_select = ""
        trace_inner_select = ""
        trace_group = ""
        entity_fragment = ""
        if entities is not None:
            params["annotation_trace_ids"] = tuple(
                dict.fromkeys(trace_id for trace_id, _ in entities)
            )
            params["annotation_span_entities"] = entities
            trace_select = "toString(trace_id) AS trace_id,"
            trace_inner_select = "trace_id,"
            trace_group = "trace_id,"
            entity_fragment = """
              AND NOT isNull(trace_id)
              AND toString(trace_id) IN %(annotation_trace_ids)s
              AND (toString(trace_id), observation_span_id) IN %(annotation_span_entities)s
            """

        # Candidate-scoped latest replay. Deletion predicates remain outside
        # LIMIT 1 BY id so an unmerged tombstone cannot resurrect an old score.
        query = f"""
        SELECT
            {trace_select}
            observation_span_id,
            toString(label_id) AS label_id,
            anyLast(value) AS value
        FROM (
            SELECT {trace_inner_select} observation_span_id, label_id, value,
                   _peerdb_is_deleted AS latest_cdc_deleted,
                   deleted AS latest_soft_deleted
            FROM {self.ANNOTATION_TABLE}
            WHERE observation_span_id IN %(span_ids)s
              {entity_fragment}
              AND label_id IN %(label_ids)s
              {created_fragment}
            ORDER BY _peerdb_version DESC
            LIMIT 1 BY id
        )
        WHERE latest_cdc_deleted = 0
          AND latest_soft_deleted = false
        GROUP BY {trace_group}observation_span_id, label_id
        """
        return query, params

    # ------------------------------------------------------------------
    # Result merging
    # ------------------------------------------------------------------

    @staticmethod
    def pivot_eval_results(
        eval_rows: list[dict],
        *,
        key_by_trace: bool = False,
    ) -> dict[Any, dict[str, Any]]:
        """Pivot eval query results into a nested dict keyed by span_id.

        Returns:
            ``{span_id: {eval_config_id: cell_value}}``. The value is a number
            for completed evals, ``{"error": True}`` when all rows errored, or a
            ``{"status": "skipped"|"running"|"pending"}`` marker (with
            ``skipped_reason`` when skipped) when the (span, config) pair has no
            completed result yet. For CHOICES evals (non-empty ``str_lists``) the
            value is a ``{choice: pct}`` dict the caller spreads into
            ``{config_id}**{choice}`` keys.
        """
        import json as _json

        result: dict[Any, dict[str, Any]] = {}
        for row in eval_rows:
            span_id = str(row.get("observation_span_id", ""))
            trace_id = str(row.get("trace_id") or "")
            if key_by_trace and (not trace_id or not span_id):
                # A bare OTel span ID cannot prove ownership. Exact-identity
                # callers intentionally exclude legacy rows lacking trace_id.
                continue
            span_key: Any = (trace_id, span_id) if key_by_trace else span_id
            config_id = str(row.get("eval_config_id", ""))
            avg_score = row.get("avg_score")
            pass_rate = row.get("pass_rate")
            success_count = row.get("success_count", 0) or 0
            error_count = row.get("error_count", 0) or 0
            str_lists = row.get("str_lists") or []

            # All rows errored — surface an explicit error marker so the
            # UI can render an error state (distinct from "no eval run").
            if success_count == 0 and error_count > 0:
                result.setdefault(span_key, {})[config_id] = {"error": True}
                continue

            # CHOICES eval: compute per-choice percentage across all
            # non-errored eval rows for this (span, config) pair.
            #
            # ClickHouse stores ``output_str_list`` as ``String DEFAULT '[]'``,
            # so non-CHOICES evals (Pass/Fail, score) come back as the string
            # ``'[]'`` — truthy, slipping past the ``if not sl`` guard. Only
            # treat entries with actual choice values as CHOICES data; empty
            # inner lists must fall through to ``avg_score``/``pass_rate``.
            parsed = []
            for sl in str_lists:
                if not sl:
                    continue
                if isinstance(sl, list):
                    if sl:
                        parsed.append([str(x) for x in sl])
                elif isinstance(sl, str) and sl.startswith("["):
                    try:
                        p = _json.loads(sl)
                        if isinstance(p, list) and p:
                            parsed.append([str(x) for x in p])
                    except _json.JSONDecodeError:
                        continue
            if parsed:
                total = len(parsed)
                counts: dict[str, int] = {}
                for lst in parsed:
                    for choice in set(lst):
                        counts[choice] = counts.get(choice, 0) + 1
                per_choice = {k: round(100.0 * v / total, 2) for k, v in counts.items()}
                result.setdefault(span_key, {})[config_id] = per_choice
                continue

            # Determine the score value matching PG format
            if avg_score is not None and avg_score != 0:
                score = round(avg_score * 100, 2)
            elif pass_rate is not None:
                score = round(pass_rate, 2)
            else:
                score = None

            # No completed score: surface a non-terminal / skipped lifecycle
            # marker (skipped > running > pending) so the cell renders a
            # loading/pending/skipped state instead of a misleading blank.
            if score is None:
                result.setdefault(span_key, {})[config_id] = non_terminal_eval_marker(
                    row
                )
            else:
                result.setdefault(span_key, {})[config_id] = score

        return result

    @staticmethod
    def pivot_annotation_results(
        annotation_rows: list[dict],
        label_types: dict[str, str] | None = None,
        *,
        key_by_trace: bool = False,
    ) -> dict[Any, dict[str, Any]]:
        """Pivot annotation query results into a nested dict keyed by span_id.

        Args:
            annotation_rows: Rows from the Phase-3 query.
            label_types: Optional mapping of label_id -> annotation type
                (NUMERIC, STAR, THUMBS_UP_DOWN, CATEGORICAL).

        Returns:
            ``{span_id: {label_id: annotation_value}}``.
        """
        import json

        label_types = label_types or {}
        result: dict[Any, dict[str, Any]] = {}
        for row in annotation_rows:
            span_id = str(row.get("observation_span_id", ""))
            trace_id = str(row.get("trace_id") or "")
            if key_by_trace and (not trace_id or not span_id):
                continue
            span_key: Any = (trace_id, span_id) if key_by_trace else span_id
            label_id = str(row.get("label_id", ""))
            label_type = label_types.get(label_id, "").lower()

            raw_val = row.get("value", "{}")
            if isinstance(raw_val, str):
                try:
                    val = json.loads(raw_val)
                except (json.JSONDecodeError, TypeError):
                    val = {}
            else:
                val = raw_val if isinstance(raw_val, dict) else {}

            if label_type in ("numeric", "star"):
                value_key = "value" if label_type == "numeric" else "rating"
                value = val.get(value_key) if isinstance(val, dict) else val
            elif label_type == "thumbs_up_down":
                thumb_val = val.get("value") if isinstance(val, dict) else val
                value = thumb_val in (True, "up", 1, "true")
            elif label_type == "categorical":
                value = val.get("selected", []) if isinstance(val, dict) else val
            elif label_type == "text":
                value = val.get("text", val) if isinstance(val, dict) else val
            else:
                value = val

            result.setdefault(span_key, {})[label_id] = value

        return result
