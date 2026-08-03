"""
Trace List Query Builder for ClickHouse.

Replaces the ``list_traces()`` method in ``tracer.views.trace`` with a
two-phase ClickHouse query strategy:

Phase 1 -- Paginated trace IDs + root span data from the denormalized
``spans`` table (``WHERE parent_span_id IS NULL``).

Phase 2 -- Eval scores from ``tracer_eval_logger FINAL`` for those
trace IDs, grouped by ``(trace_id, custom_eval_config_id)``.

The two result sets are merged in Python.
"""

import math
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
    LatestFilterPredicate,
    partition_trace_filter_plans,
    supports_trace_filters,
    targets_trace_filter_domain,
)

# On the v2 schema (PARTITION BY toDate(start_time), PK on toStartOfHour(
# start_time)) start_time prunes partitions and the PK; created_at prunes
# nothing and scans the whole project.
TIME_FILTER_COLUMN = "start_time"  # Options: "created_at" | "start_time"


def _unix_microseconds(value: datetime) -> int:
    """Encode DateTime64(6) without driver tuple-datetime precision loss."""

    utc_value = (
        value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    )
    delta = utc_value - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


class TraceListQueryBuilder(BaseQueryBuilder):
    """Build queries for the paginated trace list view.

    Args:
        project_id: Project UUID string.
        page_number: Zero-based page index.
        page_size: Number of traces per page.
        filters: Frontend filter list.
        sort_params: Frontend sort specification list.
        eval_config_ids: List of ``CustomEvalConfig`` UUID strings to
            fetch eval scores for.
    """

    TABLE = "spans"
    EVAL_TABLE = "tracer_eval_logger"
    # Filter compiler class; the v2 list builder overrides this to the v2
    # builder so it reads the v2 dimension tables (end_users, etc.).
    _FILTER_BUILDER_CLS = ClickHouseFilterBuilder

    # Mapping from sort column names the frontend sends to actual
    # ClickHouse column names on the root span.
    SORT_FIELD_MAP: dict[str, str] = {
        "created_at": "start_time",
        "start_time": "start_time",
        "latency": "latency_ms",
        "latency_ms": "latency_ms",
        "cost": "cost",
        "total_tokens": "total_tokens",
        "name": "trace_name",
        "trace_name": "trace_name",
        "status": "status",
    }

    # All available light columns for configurable column selection.
    AVAILABLE_COLUMNS: list[str] = [
        "trace_id",
        "trace_name",
        "name",
        "observation_type",
        "status",
        "start_time",
        "end_time",
        "latency_ms",
        "cost",
        "total_tokens",
        "prompt_tokens",
        "completion_tokens",
        "model",
        "provider",
        "trace_session_id",
        "project_id",
    ]

    def __init__(
        self,
        project_id: str | None = None,
        project_ids: list[str] | None = None,
        page_number: int = 0,
        page_size: int = 50,
        filters: list[dict] | None = None,
        sort_params: list[dict] | None = None,
        eval_config_ids: list[str] | None = None,
        project_version_id: str | None = None,
        search: str | None = None,
        columns: list[str] | None = None,
        annotation_label_ids: list[str] | None = None,
        bounded_internal_scan: bool = False,
        bounded_identity_only: bool = False,
        bounded_bulk_scan: bool = False,
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
        self.project_version_id = project_version_id
        self.search = search.strip() if search else None
        self.columns = columns
        self.annotation_label_ids = annotation_label_ids or []
        self._bounded_internal_scan = bool(bounded_internal_scan)
        self._bounded_identity_only = bool(bounded_identity_only)
        self._bounded_bulk_scan = bool(bounded_bulk_scan)
        if self._bounded_bulk_scan and not self._bounded_identity_only:
            raise ValueError("bounded_bulk_scan requires bounded_identity_only")
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

    def supports_bounded_filter_scan(self) -> bool:
        """Whether the latest-state bounded reader can represent this request."""

        bounded_filters = self._bounded_filters()
        try:
            partition_trace_filter_plans(bounded_filters)
        except (TypeError, ValueError):
            return False
        return (
            supports_trace_filters(bounded_filters)
            and self.bounded_filter_degraded_error_code() is None
        )

    def _bounded_filters(self) -> list[dict[str, Any]]:
        """Represent free-text search as a literal latest-root predicate.

        The legacy ``ILIKE`` query scanned the full requested window and also
        interpreted user ``%``/``_`` characters as wildcards.  The bounded
        selector instead treats the search value as a literal, case-insensitive
        ``trace_name`` substring.  Reusing the normal root-filter compiler keeps
        raw seed pruning and latest-state classification identical to an
        explicit trace-name filter without mutating the public filter payload.
        """

        filters = list(self.filters)
        if self.search:
            filters.append(
                {
                    "column_id": "trace_name",
                    "filter_config": {
                        "col_type": "SYSTEM_METRIC",
                        "filter_type": "text",
                        "filter_op": "contains",
                        "filter_value": self.search,
                    },
                }
            )
        return filters

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

        # The bounded reader has one fixed newest-first order.  Free-text
        # search is compiled as a root predicate above; an arbitrary custom
        # sort still cannot be answered in that hard-coded order.
        if self.sort_params:
            return "unsupported_filter_modifiers"
        if not self._active_non_time_filters() and not self.search:
            return None
        if not supports_trace_filters(self._bounded_filters()):
            return (
                "unsupported_filter_shape"
                if targets_trace_filter_domain(self.filters)
                else None
            )
        return None

    def filter_seed_proves_result_order(self) -> bool:
        """Only root seeds can prove a canonical root-order prefix.

        Any-span filters seed the directly-indexable matching child span.
        Child order is unrelated to root order, so those reads exhaust the
        complete request window before returning page 1 or page N.
        """

        plans, _ = partition_trace_filter_plans(self._bounded_filters())
        return not any(plan.scope == "any" for plan in plans)

    def _filter_anchor_plans(self) -> list[LatestFilterPredicate]:
        """Return directly selective any-span leaves safe for a broad probe.

        Typed Map/system predicates can use deployed skip indexes and stop at
        the 513-row sentinel.  Structured JSON extraction has no such index;
        probing it across the complete UI window was itself the expensive
        query and could consume the endpoint deadline before the bounded
        root-ordered fallback ran.
        """

        plans, _ = partition_trace_filter_plans(self._bounded_filters())
        return [
            plan
            for plan in plans
            if plan.scope == "any" and "JSONExtract" not in plan.seed_predicate
        ]

    def _has_unindexed_any_span_filter(self) -> bool:
        plans, _ = partition_trace_filter_plans(self._bounded_filters())
        return any(
            plan.scope == "any" and "JSONExtract" in plan.seed_predicate
            for plan in plans
        )

    def recommended_filter_seed_batch_size(self) -> int:
        """Use the production-proven finite 512-trace seed/classifier batch."""

        plans, _ = partition_trace_filter_plans(self._bounded_filters())
        if self._has_unindexed_any_span_filter():
            return 50
        return 512 if any(plan.scope == "any" for plan in plans) else 50

    def recommended_filter_classify_batch_size(self) -> int | None:
        """Keep the candidate-trace latest-state scan below CH's memory ceiling."""

        # Normal list/graph classifiers hydrate the complete light root row and
        # stay at the production-proven 50-trace ceiling.  ID-only internal
        # consumers (eval/task and bulk selection) project just trace_id +
        # start_time, so they can use the selector's 200-candidate batch without
        # materialising the presentation columns for every candidate.
        if self._bounded_bulk_scan:
            return 200
        plans, _ = partition_trace_filter_plans(self._bounded_filters())
        if self._has_unindexed_any_span_filter():
            return 50
        return 512 if any(plan.scope == "any" for plan in plans) else 50

    def bounded_filter_seed_identity(
        self, row: dict[str, Any]
    ) -> tuple[str, str, str, Any] | str:
        """Keyset selective seeds by physical span, public rows by trace."""

        if row.get("matched_span_id"):
            return (
                str(row.get("project_id") or self.project_id or ""),
                str(row.get("trace_id") or ""),
                str(row.get("matched_span_id") or ""),
                row.get("start_time"),
            )
        return str(row.get("trace_id") or "")

    @staticmethod
    def bounded_filter_seed_order_token(
        row: dict[str, Any],
    ) -> tuple[str, str, str] | str:
        if row.get("matched_span_id"):
            return (
                str(row.get("matched_span_id") or ""),
                str(row.get("trace_id") or ""),
                str(row.get("project_id") or ""),
            )
        return str(row.get("trace_id") or "")

    def supports_filter_anchor_probe(self) -> bool:
        """Whether a direct any-span leaf can classify sparse vs common."""

        return bool(self._filter_anchor_plans())

    def build_filter_anchor_probe(self, *, limit: int) -> tuple[str, dict[str, Any]]:
        """Return a finite unordered any-span candidate sentinel.

        ``DISTINCT ... LIMIT`` can stop after the sentinel and uses the
        deployed Map key/value bloom expressions directly. It deliberately
        does not ``GROUP BY`` or order the full match set: both forms scanned
        the complete input on production and exceeded the 512 MiB read budget.
        Every row is only a superset seed; the finite classifier resolves its
        physical latest state before it can become a result.
        """

        if limit <= 1:
            raise ValueError("anchor probe limit must include a sentinel")
        request_start, request_end = self.parse_time_range(self.filters)
        self.start_date, self.end_date = request_start, request_end
        anchor_plans = self._filter_anchor_plans()
        if not anchor_plans:
            raise ValueError("trace anchor probe requires an indexed any-span filter")
        anchor = anchor_plans[0]
        anchor_params = {
            key: value
            for key, value in anchor.params.items()
            if f"%({key})s" in anchor.seed_predicate
        }
        params: dict[str, Any] = {
            **self.params,
            **anchor_params,
            "filter_anchor_start": request_start,
            "filter_anchor_end": request_end,
            "filter_anchor_limit": int(limit),
        }
        project_version_fragment = ""
        if self.project_version_id:
            params["project_version_id"] = self.project_version_id
            project_version_fragment = "AND project_version_id = %(project_version_id)s"
        sampling_fragment = ""
        if self._bounded_sampling_rate is not None:
            params["bounded_sampling_salt"] = str(self._bounded_sampling_salt)
            params["bounded_sampling_rate"] = float(self._bounded_sampling_rate)
            sampling_fragment = """
              AND modulo(
                  cityHash64(%(bounded_sampling_salt)s, toString(trace_id)), 100
              ) < %(bounded_sampling_rate)s
            """
        query = f"""
        SELECT DISTINCT trace_id
        FROM {self.TABLE}
        PREWHERE {self.project_filter_sql()}
          AND is_deleted = 0
          {project_version_fragment}
          AND start_time >= %(filter_anchor_start)s
          AND start_time < %(filter_anchor_end)s
        WHERE {anchor.seed_predicate}
          {sampling_fragment}
        LIMIT %(filter_anchor_limit)s
        """
        return query, params

    def build_filter_ordered_seed_page(
        self,
        *,
        slice_start: datetime,
        slice_end: datetime,
        limit: int,
        before_start_time: datetime | None = None,
        before_id: Any = None,
    ) -> tuple[str, dict[str, Any]]:
        """Return a root-ordered superset after a common anchor sentinel.

        This path never builds an unbounded trace-id Set. Finite roots are
        classified against all any-span filters, and the reader stops only
        when the returned root prefix is mathematically closed.
        """

        if limit <= 0:
            raise ValueError("limit must be greater than zero")
        if (before_start_time is None) != (before_id is None):
            raise ValueError("trace keyset values must be provided together")
        request_start, request_end = self.parse_time_range(self.filters)
        if not request_start <= slice_start < slice_end <= request_end:
            raise ValueError("trace seed slice must stay inside the request window")
        self.start_date, self.end_date = request_start, request_end
        plans, _ = partition_trace_filter_plans(self._bounded_filters())
        root_plans = [plan for plan in plans if plan.scope == "root"]
        params: dict[str, Any] = {
            **self.params,
            "filter_slice_start": slice_start,
            "filter_slice_end": slice_end,
            "filter_seed_limit": int(limit),
        }
        for plan in root_plans:
            params.update(
                {
                    key: value
                    for key, value in plan.params.items()
                    if f"%({key})s" in plan.seed_predicate
                }
            )
        root_predicate = " AND ".join(plan.seed_predicate for plan in root_plans)
        predicate_fragment = f"AND {root_predicate}" if root_predicate else ""
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="start_time",
                param_prefix="trace_ordered_time_exclusion",
            )
        )
        params.update(datetime_params)
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )
        project_version_fragment = ""
        if self.project_version_id:
            params["project_version_id"] = self.project_version_id
            project_version_fragment = "AND project_version_id = %(project_version_id)s"
        sampling_fragment = ""
        if self._bounded_sampling_rate is not None:
            params["bounded_sampling_salt"] = str(self._bounded_sampling_salt)
            params["bounded_sampling_rate"] = float(self._bounded_sampling_rate)
            sampling_fragment = """
              AND modulo(
                  cityHash64(%(bounded_sampling_salt)s, toString(trace_id)), 100
              ) < %(bounded_sampling_rate)s
            """
        keyset_fragment = ""
        if before_start_time is not None:
            if not slice_start <= before_start_time < slice_end:
                raise ValueError("trace keyset must stay inside its slice")
            params["filter_before_start_us"] = _unix_microseconds(before_start_time)
            params["filter_before_id"] = str(before_id)
            keyset_fragment = """
              AND (
                  toUnixTimestamp64Micro(start_time) < %(filter_before_start_us)s
                  OR (
                      toUnixTimestamp64Micro(start_time) = %(filter_before_start_us)s
                      AND trace_id < %(filter_before_id)s
                  )
              )
            """
        query = f"""
        SELECT trace_id, id AS root_span_id, start_time
        FROM {self.TABLE}
        PREWHERE {self.project_filter_sql()}
          AND is_deleted = 0
          {project_version_fragment}
          AND (parent_span_id IS NULL OR parent_span_id = '')
          AND start_time >= %(filter_slice_start)s
          AND start_time < %(filter_slice_end)s
        WHERE 1 = 1
          {predicate_fragment}{datetime_fragment}
          {sampling_fragment}
          {keyset_fragment}
        ORDER BY start_time DESC, trace_id DESC
        LIMIT 1 BY trace_id
        LIMIT %(filter_seed_limit)s
        """
        return query, params

    def build_filter_seed_page(
        self,
        *,
        slice_start: datetime,
        slice_end: datetime,
        limit: int,
        before_start_time: datetime | None = None,
        before_id: Any = None,
    ) -> tuple[str, dict[str, Any]]:
        """Return a bounded root-order superset for latest-state classification."""

        if not self.supports_bounded_filter_scan():
            raise ValueError("unsupported bounded trace filter scan")
        if limit <= 0:
            raise ValueError("limit must be greater than zero")
        if (before_start_time is None) != (before_id is None):
            raise ValueError("trace keyset values must be provided together")

        request_start, request_end = self.parse_time_range(self.filters)
        if not request_start <= slice_start < slice_end <= request_end:
            raise ValueError("trace seed slice must stay inside the request window")
        self.start_date, self.end_date = request_start, request_end
        self.params.update({"start_date": request_start, "end_date": request_end})

        plans, _ = partition_trace_filter_plans(self._bounded_filters())
        root_plans = [plan for plan in plans if plan.scope == "root"]
        any_span_plans = [plan for plan in plans if plan.scope == "any"]
        # One directly-indexable any-span leaf is a complete candidate anchor:
        # every trace satisfying all filters must contain a span satisfying
        # this leaf. The classifier below applies every leaf against global
        # latest state. Applying all leaves here would be wrong because two
        # different child spans may satisfy two different trace filters.
        seed_plans = [any_span_plans[0]] if any_span_plans else root_plans
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
        for plan in seed_plans:
            params.update(
                {
                    key: value
                    for key, value in plan.params.items()
                    if f"%({key})s" in plan.seed_predicate
                }
            )

        predicate = " AND ".join(plan.seed_predicate for plan in seed_plans)
        predicate_fragment = f"AND {predicate}" if predicate else ""
        # Trace datetime leaves bind to the displayed root timestamp. An
        # any-span seed is only a superset, so defer the complement to root
        # classification; applying it to the matching child could hide a
        # trace whose root is valid.
        datetime_predicate = ""
        if not any_span_plans:
            datetime_predicate, datetime_params = (
                BaseQueryBuilder.bounded_datetime_exclusion_sql(
                    self.filters,
                    column="start_time",
                    param_prefix="trace_seed_time_exclusion",
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
                  cityHash64(%(bounded_sampling_salt)s, toString(trace_id)), 100
              ) < %(bounded_sampling_rate)s
            """

        keyset_fragment = ""
        if before_start_time is not None:
            if not slice_start <= before_start_time < slice_end:
                raise ValueError("trace keyset must stay inside its slice")
            params["filter_before_start_us"] = _unix_microseconds(before_start_time)
            if any_span_plans:
                if not (
                    isinstance(before_id, tuple)
                    and len(before_id) == 3
                    and all(isinstance(value, str) for value in before_id)
                ):
                    raise ValueError(
                        "any-span keyset must be an (id, trace_id, project_id) tuple"
                    )
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
            else:
                params["filter_before_id"] = str(before_id)
                keyset_fragment = """
              AND (
                  toUnixTimestamp64Micro(start_time) < %(filter_before_start_us)s
                  OR (
                      toUnixTimestamp64Micro(start_time) = %(filter_before_start_us)s
                      AND trace_id < %(filter_before_id)s
                  )
              )
            """

        if any_span_plans:
            select_fragment = "project_id, trace_id, id AS matched_span_id, start_time"
            root_fragment = ""
            order_fragment = (
                "ORDER BY start_time DESC, id DESC, trace_id DESC, project_id DESC"
            )
            limit_by_fragment = "LIMIT 1 BY project_id, trace_id, id, start_time"
        else:
            select_fragment = "trace_id, id AS root_span_id, start_time"
            root_fragment = "AND (parent_span_id IS NULL OR parent_span_id = '')"
            order_fragment = "ORDER BY start_time DESC, trace_id DESC"
            limit_by_fragment = "LIMIT 1 BY trace_id"

        query = f"""
        SELECT {select_fragment}
        FROM {self.TABLE}
        PREWHERE {self.project_filter_sql()}
          AND is_deleted = 0
          {project_version_fragment}
          {root_fragment}
          AND start_time >= %(filter_slice_start)s
          AND start_time < %(filter_slice_end)s
        WHERE 1 = 1
          {predicate_fragment}{datetime_fragment}
          {sampling_fragment}
          {keyset_fragment}
        {order_fragment}
        {limit_by_fragment}
        LIMIT %(filter_seed_limit)s
        """
        return query, params

    def build_filter_match_query(
        self,
        candidate_ids: list[str],
        *,
        candidate_full_state: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        """Classify bounded trace IDs against their latest span versions.

        Direct-write OTLP rows treat ``(project_id, trace_id, id, start_time)``
        as immutable identity. Payload columns and tombstones may acquire newer
        versions and are resolved with ``argMax`` below.  One candidate-trace
        scan resolves every physical span, selects the newest live root, and
        evaluates any-span membership.  This avoids both the production-timeout
        nested physical-ID set and a false negative when the raw seed root was
        tombstoned but another root for the trace remains live.
        """

        trace_ids = tuple(dict.fromkeys(str(value) for value in candidate_ids if value))
        if not trace_ids:
            return "", {}
        candidate_limit = 200 if self._bounded_bulk_scan else 512
        if len(trace_ids) > candidate_limit:
            raise ValueError("candidate trace batch exceeds bounded limit")
        if not self.supports_bounded_filter_scan():
            raise ValueError("unsupported bounded trace filter scan")

        request_start, request_end = self.parse_time_range(self.filters)
        self.start_date, self.end_date = request_start, request_end
        self.params.update({"start_date": request_start, "end_date": request_end})
        has_explicit_time_filter = any(
            (item.get("column_id") or item.get("columnId"))
            in {"created_at", "start_time"}
            for item in self.filters
        )
        # A continuous-task classifier receives identities from a separate
        # arrival/change seed.  Its default 30-day UI window is not membership:
        # an old span updated now must still be replayed against latest state.
        # Preserve an explicit user time filter, however, because that *is*
        # part of the task's selection contract.
        scope_to_request_window = not candidate_full_state or has_explicit_time_filter
        plans, residual_filters = partition_trace_filter_plans(self._bounded_filters())
        root_plans = [plan for plan in plans if plan.scope == "root"]
        any_span_plans = [plan for plan in plans if plan.scope == "any"]
        params: dict[str, Any] = {
            **self.params,
            "candidate_trace_ids": trace_ids,
        }
        if scope_to_request_window:
            params.update(
                {
                    "candidate_start_date": request_start,
                    "candidate_end_date": request_end,
                }
            )
        candidate_time_fragment = ""
        if scope_to_request_window:
            # start_time is part of the immutable physical span identity. All
            # versions of an in-window span therefore stay in the same daily
            # partition, so pruning outside partitions before argMax cannot
            # hide a newer version or change latest-state membership.
            candidate_time_fragment = """
                  AND toDate(start_time) >= toDate(%(candidate_start_date)s)
                  AND toDate(start_time) <= toDate(%(candidate_end_date)s)
                  AND start_time >= %(candidate_start_date)s
                  AND start_time < %(candidate_end_date)s
            """
        project_version_fragment = ""
        if self.project_version_id:
            params["project_version_id"] = self.project_version_id
            project_version_fragment = "AND project_version_id = %(project_version_id)s"
        for plan in plans:
            params.update(plan.params)

        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column="latest_start_time",
                param_prefix="trace_match_time_exclusion",
            )
        )
        params.update(datetime_params)
        datetime_root_fragment = (
            f"\n                    AND {datetime_predicate}"
            if datetime_predicate
            else ""
        )

        plan_aggregates = [aggregate for plan in plans for aggregate in plan.aggregates]
        plan_aggregate_fragment = (
            ",\n                        "
            + ",\n                        ".join(plan_aggregates)
            if plan_aggregates
            else ""
        )
        root_aggregate_aliases = [
            aggregate.rsplit(" AS ", 1)[1].strip()
            for plan in root_plans
            for aggregate in plan.aggregates
        ]
        if any(not alias for alias in root_aggregate_aliases):
            raise AssertionError("root latest-state aggregate requires an alias")

        if scope_to_request_window:
            canonical_root_condition = f"""(
                    latest_start_time >= %(candidate_start_date)s
                    AND latest_start_time < %(candidate_end_date)s{datetime_root_fragment}
                    AND (
                        latest_parent_span_id IS NULL
                        OR latest_parent_span_id = ''
                    )
                )"""
        else:
            canonical_root_condition = """(
                    latest_parent_span_id IS NULL
                    OR latest_parent_span_id = ''
                )"""
        canonical_root_order = "tuple(latest_start_time, grouped_id)"
        canonical_root_aggregates = [
            (
                f"argMaxIf(tuple({alias}), {canonical_root_order}, "
                f"{canonical_root_condition}).1 AS {alias}"
            )
            for alias in root_aggregate_aliases
        ]
        canonical_root_aggregate_fragment = (
            ",\n                "
            + ",\n                ".join(canonical_root_aggregates)
            if canonical_root_aggregates
            else ""
        )
        root_predicate = " AND ".join(plan.predicate for plan in root_plans) or "1 = 1"

        # ``start_time`` is part of the immutable physical span identity
        # ``(project_id, trace_id, id, start_time)``.  Gate each any-span leaf
        # after version collapse, not only the canonical root: a stale raw
        # in-window seed must not let a different current span outside the
        # requested half-open window satisfy the trace filter.
        if scope_to_request_window:
            any_span_window_condition = """(
                    latest_start_time >= %(candidate_start_date)s
                    AND latest_start_time < %(candidate_end_date)s
                )"""
            any_span_having = " AND ".join(
                (f"countIf({any_span_window_condition} AND ({plan.predicate})) > 0")
                for plan in any_span_plans
            )
        else:
            any_span_having = " AND ".join(
                f"countIf({plan.predicate}) > 0" for plan in any_span_plans
            )
        any_span_having_fragment = (
            f"\n              AND {any_span_having}" if any_span_having else ""
        )

        # Identity-only eval/task selectors must retain the exact physical
        # child span that proved each any-span filter.  A trace-level result id
        # alone cannot later bind ``final_status`` (or another child attribute)
        # to the evaluation mapping: separate leaves may be satisfied by
        # separate children, and OTel span ids can be reused across start
        # times.  Project + trace are carried by the surrounding result; each
        # tuple below preserves the remaining immutable identity fields.
        witness_selects: list[str] = []
        witness_aliases: list[str] = []
        if self._bounded_identity_only:
            for witness_index, plan in enumerate(any_span_plans):
                witness_alias = f"filter_witness_{witness_index}"
                witness_aliases.append(witness_alias)
                witness_condition = f"({plan.predicate})"
                if scope_to_request_window:
                    witness_condition = (
                        f"{any_span_window_condition} AND {witness_condition}"
                    )
                witness_selects.append(
                    "argMinIf("
                    "tuple(grouped_id, latest_start_time), "
                    "tuple(latest_start_time, grouped_id), "
                    f"{witness_condition}"
                    f") AS {witness_alias}"
                )
        witness_select_fragment = (
            ",\n                " + ",\n                ".join(witness_selects)
            if witness_selects
            else ""
        )
        witness_public_fragment = (
            ", " + ", ".join(witness_aliases) if witness_aliases else ""
        )

        residual_predicate = "1 = 1"
        if residual_filters:
            residual_builder = self._FILTER_BUILDER_CLS(
                table=self.TABLE,
                query_mode=self._FILTER_BUILDER_CLS.QUERY_MODE_TRACE,
                annotation_label_ids=self.annotation_label_ids,
                project_id=self.project_id,
                project_ids=self.project_ids,
                score_date_scope=scope_to_request_window,
                span_date_scope=scope_to_request_window,
                candidate_ids_param="candidate_trace_ids",
            )
            residual_predicate, residual_params = residual_builder.translate(
                residual_filters
            )
            params.update(residual_params)
            residual_predicate = residual_predicate or "1 = 1"

        if self._bounded_identity_only:
            per_trace_select_fragment = f"""grouped_trace_id AS trace_id,
                argMaxIf(
                    latest_start_time,
                    {canonical_root_order},
                    {canonical_root_condition}
                ) AS start_time{witness_select_fragment}"""
            public_select_fragment = f"trace_id, start_time{witness_public_fragment}"
            hydrate_root_aggregate_fragment = ""
        else:
            root_fields = (
                ("grouped_id", "root_span_id"),
                ("latest_trace_name", "trace_name"),
                ("latest_name", "span_name"),
                ("latest_observation_type", "observation_type"),
                ("latest_status", "status"),
                ("latest_start_time", "start_time"),
                ("latest_end_time", "end_time"),
                ("latest_latency_ms", "latency_ms"),
                ("latest_cost", "cost"),
                ("latest_total_tokens", "total_tokens"),
                ("latest_prompt_tokens", "prompt_tokens"),
                ("latest_completion_tokens", "completion_tokens"),
                ("latest_model", "model"),
                ("latest_provider", "provider"),
                ("latest_trace_session_id", "trace_session_id"),
                ("latest_project_id", "project_id"),
            )
            canonical_fields = [
                (
                    f"argMaxIf(tuple({source}), {canonical_root_order}, "
                    f"{canonical_root_condition}).1 AS {alias}"
                )
                for source, alias in root_fields
            ]
            per_trace_select_fragment = (
                "grouped_trace_id AS trace_id,\n                "
                + ",\n                ".join(canonical_fields)
            )
            public_select_fragment = ", ".join(
                ["root_span_id", "trace_id", *[alias for _, alias in root_fields[1:]]]
            )
            hydrate_root_aggregate_fragment = """,
                    argMax(trace_name, _peerdb_version) AS latest_trace_name,
                    argMax(name, _peerdb_version) AS latest_name,
                    argMax(observation_type, _peerdb_version)
                        AS latest_observation_type,
                    argMax(tuple(status), _peerdb_version).1 AS latest_status,
                    argMax(tuple(end_time), _peerdb_version).1 AS latest_end_time,
                    argMax(tuple(latency_ms), _peerdb_version).1
                        AS latest_latency_ms,
                    argMax(tuple(cost), _peerdb_version).1 AS latest_cost,
                    argMax(tuple(total_tokens), _peerdb_version).1
                        AS latest_total_tokens,
                    argMax(tuple(prompt_tokens), _peerdb_version).1
                        AS latest_prompt_tokens,
                    argMax(tuple(completion_tokens), _peerdb_version).1
                        AS latest_completion_tokens,
                    argMax(tuple(model), _peerdb_version).1 AS latest_model,
                    argMax(tuple(provider), _peerdb_version).1 AS latest_provider,
                    argMax(tuple(trace_session_id), _peerdb_version).1
                        AS latest_trace_session_id,
                    argMax(project_id, _peerdb_version) AS latest_project_id"""

        query = f"""
        SELECT {public_select_fragment}
        FROM (
            SELECT
                {per_trace_select_fragment}
                {canonical_root_aggregate_fragment}
            FROM (
                SELECT
                    id AS grouped_id,
                    trace_id AS grouped_trace_id,
                    argMax(tuple(parent_span_id), _peerdb_version).1
                        AS latest_parent_span_id,
                    start_time AS latest_start_time,
                    argMax(is_deleted, _peerdb_version) AS latest_is_deleted
                    {hydrate_root_aggregate_fragment}
                    {plan_aggregate_fragment}
                FROM {self.TABLE}
                PREWHERE {self.project_filter_sql()}
                  {project_version_fragment}
                  AND trace_id IN %(candidate_trace_ids)s
                  {candidate_time_fragment}
                GROUP BY trace_id, id, start_time
            )
            WHERE latest_is_deleted = 0
              AND grouped_trace_id IN %(candidate_trace_ids)s
            GROUP BY grouped_trace_id
            HAVING countIf({canonical_root_condition}) > 0
              AND {root_predicate}
              {any_span_having_fragment}
        ) AS latest_candidates
        WHERE {residual_predicate}
        ORDER BY start_time DESC, trace_id DESC
        LIMIT {len(trace_ids)}
        """
        return query, params

    def build_filter_match_query_from_seed_rows(
        self,
        candidate_rows: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any]]:
        """Replay root-seeded candidates by bounded trace identity.

        A raw seed root can have a newer tombstone while another physical root
        for the same trace remains live, so its root ID is deliberately not a
        classifier constraint.
        """

        trace_ids = [str(row.get("trace_id") or "") for row in candidate_rows]
        return self.build_filter_match_query(trace_ids)

    def _span_time_window(
        self, params: dict[str, Any], column: str = "start_time"
    ) -> str:
        """Bound a page-scoped span probe to the request window ± 1 day.

        Page trace_ids come from the windowed page scan; every span of an
        in-window trace starts within the window ± max trace duration (prod
        max ≈ 5h « 1d). Empty when no build() ran (standalone callers).
        """
        if self.start_date is None:
            return ""
        params["start_date"] = self.start_date
        params["end_date"] = self.end_date
        return (
            f"AND {column} >= %(start_date)s - INTERVAL 1 DAY\n"
            f"          AND {column} < %(end_date)s + INTERVAL 1 DAY"
        )

    # ------------------------------------------------------------------
    # Phase 1: Paginated trace list
    # ------------------------------------------------------------------

    def build(self) -> tuple[str, dict[str, Any]]:
        """Build the Phase-1 query for paginated root-span trace data.

        Returns:
            A ``(query_string, params)`` tuple.  The query returns one row
            per trace with root-span metadata.
        """
        if self.search:
            raise ValueError(
                "unsafe legacy filtered trace read blocked: bounded_search_required"
            )
        if error_code := self.bounded_filter_degraded_error_code():
            raise ValueError(f"unsafe legacy filtered trace read blocked: {error_code}")
        self.start_date, self.end_date = self.parse_time_range(self.filters)
        self.params["start_date"] = self.start_date
        self.params["end_date"] = self.end_date

        # Translate attribute / metric filters
        fb = self._FILTER_BUILDER_CLS(
            table=self.TABLE,
            annotation_label_ids=self.annotation_label_ids,
            project_id=self.project_id,
            project_ids=self.project_ids,
            # PERF: bound the trace-membership span subqueries the compiler
            # emits (model/status/attr/user filters) to the query's time
            # window — without this each filter scans the project's entire
            # span history. Safe here: this builder always binds
            # %(start_date)s before translate(). See filters.py.
            span_date_scope=True,
        )
        extra_where, extra_params = fb.translate(self.filters)
        self.params.update(extra_params)
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column=TIME_FILTER_COLUMN,
                param_prefix="trace_list_time_exclusion",
            )
        )
        self.params.update(datetime_params)

        # Sorting
        order_clause = fb.translate_sort(
            self.sort_params, field_map=self.SORT_FIELD_MAP
        )
        if not order_clause:
            order_clause = "ORDER BY start_time DESC"

        # Prefix-fetch pagination: read the sorted prefix [0, offset +
        # 2*page_size) in ONE bounded top-K pass and let the view dedup by
        # trace id then slice [offset, offset + page_size) — see
        # tracer/services/clickhouse/page_dedup.py. Preserves the global
        # dedup `LIMIT 1 BY trace_id` provided (a trace — even a multi-root
        # one whose roots sort pages apart — can never appear on two pages)
        # without its O(window) full sort. No SQL OFFSET; slicing in Python.
        offset = self.page_number * self.page_size
        self.params["limit"] = offset + 2 * self.page_size

        # Build optional filter fragment
        filter_fragment = f"AND {extra_where}" if extra_where else ""
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )

        # Optional project_version_id filter (used by prototype tab)
        pv_fragment = ""
        if self.project_version_id:
            pv_fragment = "AND project_version_id = %(project_version_id)s"
            self.params["project_version_id"] = self.project_version_id

        # Search filter on trace_name
        search_fragment = ""
        if self.search:
            search_fragment = "AND trace_name ILIKE %(search)s"
            self.params["search"] = f"%{self.search}%"

        # Configurable columns — only SELECT requested columns.
        # trace_id is always included.
        if self.columns:
            valid = [c for c in self.columns if c in self.AVAILABLE_COLUMNS]
            if "trace_id" not in valid:
                valid.insert(0, "trace_id")
            # Alias 'name' to 'span_name' for backward compatibility
            select_cols = []
            for c in valid:
                if c == "name":
                    select_cols.append("name AS span_name")
                else:
                    select_cols.append(c)
            select_clause = ",\n            ".join(select_cols)
        else:
            select_clause = """trace_id,
            trace_name,
            name AS span_name,
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
            trace_session_id,
            project_id"""

        # Phase 1: light columns only (no input/output/attrs/metadata).
        # Heavy columns are fetched in build_content_query() for just the
        # returned trace_ids — avoids OOM on large tables.
        #
        # PERF: no `LIMIT 1 BY trace_id`. That clause deduped multi-root /
        # duplicate-version traces, but forced CH to read + full-sort EVERY
        # root span in the window before applying ORDER BY … LIMIT —
        # O(roots-in-window) memory that OOM-crashed the server at millions
        # of traces. Without it, `ORDER BY … LIMIT n` runs as a bounded
        # top-N (size-n heap, O(n) memory). Duplicate trace_ids on a page
        # (multi-root traces, un-merged ReplacingMergeTree versions) are
        # rare; the view dedups the returned page by trace_id in Python,
        # keeping the first occurrence — the same row `LIMIT 1 BY` kept.
        query = f"""
        SELECT
            {select_clause}
        FROM {self.TABLE}
        {self.project_where()}
          AND (parent_span_id IS NULL OR parent_span_id = '')
          AND {TIME_FILTER_COLUMN} >= %(start_date)s
          AND {TIME_FILTER_COLUMN} < %(end_date)s{datetime_fragment}
          {pv_fragment}
          {search_fragment}
          {filter_fragment}
        {order_clause}
        LIMIT %(limit)s
        """
        return query, self.params

    def build_id_query(
        self,
        *,
        created_at_floor: datetime | None = None,
        created_at_ceiling: datetime | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Filtered trace ids only — same root-span predicate/window as build(),
        no pagination/order. Lets the eval resolver select the same traces this
        list endpoint returns.

        ``created_at_floor`` (continuous eval tasks only): floor the root-span
        scan on CH arrival time (``created_at``) instead of event time
        (``start_time``), so a trace whose root span landed in CH after its
        ``start_time`` is still picked up. ``None`` keeps the ``start_time``
        window used by the UI list and historical tasks.
        """
        self.start_date, self.end_date = self.parse_time_range(self.filters)
        if created_at_floor is not None:
            # Window on arrival (created_at), not start_time. NOTE: cross-table
            # filter membership subqueries (span_date_scope) still window on
            # start_time, so a filtered task can miss an arrival whose start_time
            # predates parse_time_range's window — pre-existing residual (worse
            # before this change), tracked as a follow-up.
            self.params["created_at_floor"] = created_at_floor
            time_where = "AND created_at >= %(created_at_floor)s"
            if created_at_ceiling is not None:
                self.params["created_at_ceiling"] = created_at_ceiling
                time_where += " AND created_at < %(created_at_ceiling)s"
        else:
            time_where = (
                f"AND {TIME_FILTER_COLUMN} >= %(start_date)s "
                f"AND {TIME_FILTER_COLUMN} < %(end_date)s"
            )
        self.params["start_date"] = self.start_date
        self.params["end_date"] = self.end_date

        fb = self._FILTER_BUILDER_CLS(
            table=self.TABLE,
            annotation_label_ids=self.annotation_label_ids,
            project_id=self.project_id,
            project_ids=self.project_ids,
            # PERF: bound the trace-membership span subqueries the compiler
            # emits (model/status/attr/user filters) to the query's time
            # window — without this each filter scans the project's entire
            # span history. Safe here: this builder always binds
            # %(start_date)s before translate(). See filters.py.
            span_date_scope=True,
        )
        extra_where, extra_params = fb.translate(self.filters)
        self.params.update(extra_params)
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column=TIME_FILTER_COLUMN,
                param_prefix="trace_id_time_exclusion",
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

        search_fragment = ""
        if self.search:
            search_fragment = "AND trace_name ILIKE %(search)s"
            self.params["search"] = f"%{self.search}%"

        query = f"""
        SELECT trace_id
        FROM {self.TABLE}
        {self.project_where()}
          AND (parent_span_id IS NULL OR parent_span_id = '')
          {time_where}{datetime_fragment}
          {pv_fragment}
          {search_fragment}
          {filter_fragment}
        LIMIT 1 BY trace_id
        """
        return query, self.params

    def build_content_query(
        self,
        trace_ids: list[str],
        *,
        root_identities: list[tuple[str, str, str, Any]] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Fetch heavy columns (input, output, attributes) for a page of traces.

        Resolve every physical root to its latest version before reading heavy
        payloads.  When the bounded page supplies physical root identities,
        preserve those exact roots so a reused trace ID cannot hydrate content
        from another project/version/root between Phase 1 and Phase 1b.
        """
        if not trace_ids:
            return "", {}

        params: dict[str, Any] = {
            **self.params,
            "content_trace_ids": tuple(trace_ids),
        }

        normalized_identities = tuple(
            dict.fromkeys(
                (
                    str(project_id),
                    str(trace_id),
                    str(root_span_id),
                    _unix_microseconds(start_time),
                )
                for project_id, trace_id, root_span_id, start_time in (
                    root_identities or []
                )
                if project_id and trace_id and root_span_id and start_time is not None
            )
        )
        identity_fragment = ""
        if normalized_identities:
            params["content_root_identities"] = normalized_identities
            params["content_root_dates"] = tuple(
                dict.fromkeys(
                    start_time.date()
                    for _, _, _, start_time in (root_identities or [])
                    if isinstance(start_time, datetime)
                )
            )
            identity_fragment = """
              AND toDate(start_time) IN %(content_root_dates)s
              AND (
                  toString(project_id), trace_id, id,
                  toUnixTimestamp64Micro(start_time)
              )
                    IN %(content_root_identities)s
            """

        project_version_fragment = ""
        if self.project_version_id:
            params["project_version_id"] = self.project_version_id
            project_version_fragment = (
                "AND latest_project_version_id = %(project_version_id)s"
            )

        span_window = self._span_time_window(params)
        query = f"""
        SELECT
            trace_id,
            latest_input AS input,
            latest_output AS output,
            latest_attrs_string AS attrs_string,
            latest_attrs_number AS attrs_number,
            latest_attrs_bool AS attrs_bool,
            latest_attributes_extra AS attributes_extra,
            toJSONString(latest_metadata) AS metadata,
            {self._trace_tags_select_sql()}
        FROM (
            SELECT
                project_id,
                trace_id,
                id AS root_span_id,
                start_time,
                argMax(tuple(parent_span_id), _peerdb_version).1
                    AS latest_parent_span_id,
                argMax(is_deleted, _peerdb_version) AS latest_is_deleted,
                argMax(tuple(project_version_id), _peerdb_version).1
                    AS latest_project_version_id,
                argMax(tuple(input), _peerdb_version).1 AS latest_input,
                argMax(tuple(output), _peerdb_version).1 AS latest_output,
                argMax(attrs_string, _peerdb_version) AS latest_attrs_string,
                argMax(attrs_number, _peerdb_version) AS latest_attrs_number,
                argMax(attrs_bool, _peerdb_version) AS latest_attrs_bool,
                argMax(tuple(attributes_extra), _peerdb_version).1
                    AS latest_attributes_extra,
                argMax(metadata, _peerdb_version) AS latest_metadata
            FROM {self.TABLE}
            PREWHERE trace_id IN %(content_trace_ids)s
              AND {self.project_filter_sql()}
              {identity_fragment}
              {span_window}
            GROUP BY project_id, trace_id, id, start_time
        ) AS latest_physical_roots
        {self._trace_tags_join_sql()}
        WHERE latest_is_deleted = 0
          AND (latest_parent_span_id IS NULL OR latest_parent_span_id = '')
          {project_version_fragment}
        ORDER BY start_time DESC, root_span_id DESC
        LIMIT 1 BY project_id, trace_id
        """
        return query, params

    @staticmethod
    def _trace_tags_select_sql() -> str:
        """Return the legacy trace-tag projection used outside CH25."""

        return (
            "dictGetOrDefault('trace_dict', 'tags', toUUID(trace_id), '[]') "
            "AS trace_tags"
        )

    @staticmethod
    def _trace_tags_join_sql() -> str:
        """Return an optional source join for the trace-tag projection."""

        return ""

    def build_span_attributes_query(
        self, trace_ids: list[str]
    ) -> tuple[str, dict[str, Any]]:
        """Aggregate span attributes across all spans of each trace.

        Returns one row per trace with groupArrayDistinct for each attribute key.
        Skips raw/large content keys.
        """
        if not trace_ids:
            return "", {}

        params = {**self.params, "attr_trace_ids": tuple(trace_ids)}
        span_window = self._span_time_window(params)
        query = f"""
        SELECT
            trace_id,
            attributes_extra
        FROM {self.TABLE}
        PREWHERE trace_id IN %(attr_trace_ids)s
        WHERE {self.project_filter_sql()}
          AND is_deleted = 0
          AND attributes_extra != '{{}}'
          AND attributes_extra != ''
          {span_window}
        """
        return query, params

    def build_count_query(self) -> tuple[str, dict[str, Any]]:
        """Build a query to count total matching traces (for pagination).

        Returns:
            A ``(query_string, params)`` tuple returning a single count.
        """
        if self.search:
            raise ValueError(
                "unsafe legacy filtered trace count blocked: bounded_search_required"
            )
        if error_code := self.bounded_filter_degraded_error_code():
            raise ValueError(
                f"unsafe legacy filtered trace count blocked: {error_code}"
            )
        fb = self._FILTER_BUILDER_CLS(
            table=self.TABLE,
            annotation_label_ids=self.annotation_label_ids,
            project_id=self.project_id,
            project_ids=self.project_ids,
            # PERF: bound the trace-membership span subqueries the compiler
            # emits (model/status/attr/user filters) to the query's time
            # window — without this each filter scans the project's entire
            # span history. Safe here: this builder always binds
            # %(start_date)s before translate(). See filters.py.
            span_date_scope=True,
        )
        extra_where, extra_params = fb.translate(self.filters)
        # Merge params -- reuse the same start/end dates
        params = dict(self.params)
        params.update(extra_params)
        datetime_predicate, datetime_params = (
            BaseQueryBuilder.bounded_datetime_exclusion_sql(
                self.filters,
                column=TIME_FILTER_COLUMN,
                param_prefix="trace_count_time_exclusion",
            )
        )
        params.update(datetime_params)

        filter_fragment = f"AND {extra_where}" if extra_where else ""
        datetime_fragment = (
            f"\n          AND {datetime_predicate}" if datetime_predicate else ""
        )

        # Optional project_version_id filter
        pv_fragment = ""
        if self.project_version_id:
            pv_fragment = "AND project_version_id = %(project_version_id)s"
            params["project_version_id"] = self.project_version_id

        # Search filter (reuse from build())
        search_fragment = ""
        if self.search:
            search_fragment = "AND trace_name ILIKE %(search)s"
            params["search"] = f"%{self.search}%"

        query = f"""
        SELECT uniq(trace_id) AS total
        FROM {self.TABLE}
        {self.project_where()}
          AND (parent_span_id IS NULL OR parent_span_id = '')
          AND {TIME_FILTER_COLUMN} >= %(start_date)s
          AND {TIME_FILTER_COLUMN} < %(end_date)s{datetime_fragment}
          {pv_fragment}
          {search_fragment}
          {filter_fragment}
        """
        return query, params

    # ------------------------------------------------------------------
    # Span count per trace (optional — only if columns include span_count)
    # ------------------------------------------------------------------

    def build_span_count_query(
        self, trace_ids: list[str]
    ) -> tuple[str, dict[str, Any]]:
        """Count spans and errors per trace for a page of trace IDs."""
        if not trace_ids:
            return "", {}

        params: dict[str, Any] = {
            **self.params,
            "sc_trace_ids": tuple(trace_ids),
        }
        query = f"""
        SELECT
            trace_id,
            count() AS span_count,
            countIf(status = 'ERROR') AS error_count
        FROM {self.TABLE}
        WHERE {self.project_filter_sql()}
          AND trace_id IN %(sc_trace_ids)s
          AND is_deleted = 0
        GROUP BY trace_id
        """
        return query, params

    @staticmethod
    def pivot_span_count_results(
        data: list[dict],
    ) -> dict[str, dict[str, int]]:
        """Pivot span count results into ``{trace_id: {span_count, error_count}}``."""
        result: dict[str, dict[str, int]] = {}
        for row in data:
            tid = str(row.get("trace_id", ""))
            if tid:
                result[tid] = {
                    "span_count": row.get("span_count", 0),
                    "error_count": row.get("error_count", 0),
                }
        return result

    # ------------------------------------------------------------------
    # Phase 2: Eval scores for a set of trace IDs
    # ------------------------------------------------------------------

    def build_eval_query(
        self,
        trace_ids: list[str],
    ) -> tuple[str, dict[str, Any]]:
        """Build the Phase-2 eval-scores query for a page of trace IDs.

        Queries ``tracer_eval_logger FINAL`` grouped by
        ``(trace_id, custom_eval_config_id)`` to produce one aggregated
        score row per (trace, eval config) pair.

        Args:
            trace_ids: List of trace ID strings from Phase 1.

        Returns:
            A ``(query_string, params)`` tuple.  Returns empty query if
            no trace_ids or no eval_config_ids.
        """
        if not trace_ids or not self.eval_config_ids:
            return "", {}

        params: dict[str, Any] = {
            "trace_ids": tuple(trace_ids),
            "eval_config_ids": tuple(self.eval_config_ids),
        }

        # Partition-prune `tracer_eval_logger` (PARTITION BY toYYYYMM(created_at))
        # so the FINAL merge can skip months that cannot match this page.
        # The page of trace_ids was selected by build() within the user's
        # [start_date, end_date] window on `start_time`, so the matching eval
        # rows' `created_at` falls inside that window plus ingestion skew. A
        # lower-bound-only filter with a 1-day skew buffer (identical to the
        # mitigation in build()/build_count_query()) prunes old partitions
        # without dropping any legitimately-matching eval row. Guarded on
        # self.start_date so callers that invoke build_eval_query() without a
        # prior build() (e.g. unit tests) keep their current behavior.
        created_at_fragment = ""
        if self.start_date is not None:
            params["start_date"] = self.start_date
            created_at_fragment = "AND created_at >= %(start_date)s - INTERVAL 1 DAY"

        eval_table, _ = eval_logger_source(include_cdc_tombstone_guard=True)
        eval_version = eval_logger_version_column(eval_table)
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
        # non-terminal (pending/running) or skipped row never skews a score nor
        # masquerades as a real value. The per-status counts let the pivot pick
        # one cell state per (trace, config) by the precedence
        # completed > errored > skipped > running > pending.
        # ``success_count`` excludes non-terminal/skipped/errored rows via
        # ``status NOT IN (...)``: a bare ``error = 0`` guard also matches
        # pending/running/skipped rows (they carry ``error = 0`` and a NULL
        # output). NOT-IN (rather than ``status = 'completed'``) keeps legacy
        # rows whose mirrored ``status`` is empty/NULL counted as completed.
        # ``str_lists`` keeps every completed ``output_str_list`` so the pivot
        # can compute per-choice percentages for CHOICES evals.
        # ``output_str`` is Nullable(String); ClickHouse 3-valued logic makes
        # ``NULL != 'ERROR'`` NULL (not TRUE), so use ``ifNull(...)`` to keep
        # the comparison NULL-safe.
        # New per-status columns are appended after ``str_lists`` so the pivot's
        # positional column fallbacks (0..7) stay valid.
        query = f"""
        SELECT
            trace_id,
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
            count() AS eval_count,
            groupArrayIf(
                output_str_list,
                error = 0 AND ifNull(output_str, '') != 'ERROR' AND status NOT IN ('pending', 'running', 'skipped', 'errored')
            ) AS str_lists,
            countIf(status = 'skipped') AS skipped_count,
            countIf(status = 'running') AS running_count,
            countIf(status = 'pending') AS pending_count,
            anyIf(skipped_reason, status = 'skipped') AS skipped_reason
        -- Candidate-scoped latest replay: live/tombstone predicates belong
        -- outside LIMIT 1 BY id. Applying them in the inner scan resurrects an
        -- older score when its newest physical version is a deletion marker.
        FROM (
            SELECT
                trace_id,
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
            WHERE trace_id IN %(trace_ids)s
              AND custom_eval_config_id IN %(eval_config_ids)s
              {created_at_fragment}
            ORDER BY {eval_version} DESC
            LIMIT 1 BY id
        )
        WHERE {live_predicate}
        GROUP BY trace_id, custom_eval_config_id
        """
        return query, params

    # ------------------------------------------------------------------
    # Phase 3: Annotations for a set of trace IDs
    # ------------------------------------------------------------------

    ANNOTATION_TABLE = "model_hub_score"

    def build_annotation_query(
        self,
        trace_ids: list[str],
        annotation_label_ids: list[str] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Build annotation query for a page of trace IDs."""
        if not trace_ids or not annotation_label_ids:
            return "", {}

        params: dict[str, Any] = {
            "trace_ids": tuple(trace_ids),
            "label_ids": tuple(annotation_label_ids),
        }
        # Bound only the spans (sp) join side; the score (s) side keeps no
        # upper bound so annotations created after the window still resolve.
        sp_window = self._span_time_window(params, column="sp.start_time")

        query = f"""
        SELECT
            if(
                isNull(s.trace_id)
                OR s.trace_id = toUUID('00000000-0000-0000-0000-000000000000'),
                sp.trace_id,
                toString(s.trace_id)
            ) AS trace_id,
            toString(s.label_id) AS label_id,
            anyLast(s.value) AS value,
            toString(anyLast(s.annotator_id)) AS annotator_id
        FROM {self.ANNOTATION_TABLE} AS s FINAL
        LEFT JOIN {self.TABLE} AS sp
          ON sp.id = s.observation_span_id
         AND sp._peerdb_is_deleted = 0
         {sp_window}
        WHERE s._peerdb_is_deleted = 0
          AND s.deleted = false
          AND if(
                isNull(s.trace_id)
                OR s.trace_id = toUUID('00000000-0000-0000-0000-000000000000'),
                sp.trace_id,
                toString(s.trace_id)
              ) IN %(trace_ids)s
          AND s.label_id IN %(label_ids)s
        GROUP BY trace_id, label_id
        """
        return query, params

    def build_user_id_query(self, trace_ids: list[str]) -> tuple[str, dict[str, Any]]:
        """Fetch user_id strings from ClickHouse for a page of trace IDs.

        Uses enduser_dict to resolve end_user_id UUIDs to user_id strings
        in a single query. Returns one user_id per trace (uses `any()`
        aggregation to pick the first non-null value across all spans).
        """
        if not trace_ids:
            return "", {}

        params: dict[str, Any] = {
            **self.params,
            "user_trace_ids": tuple(trace_ids),
        }
        span_window = self._span_time_window(params)

        query = f"""
        SELECT trace_id, user_id
        FROM (
            SELECT
                trace_id,
                dictGetOrDefault('enduser_dict', 'user_id', any(end_user_id), '') AS user_id
            FROM {self.TABLE}
            PREWHERE trace_id IN %(user_trace_ids)s
            WHERE {self.project_filter_sql()}
              AND _peerdb_is_deleted = 0
              AND end_user_id IS NOT NULL
              AND end_user_id != toUUID('00000000-0000-0000-0000-000000000000')
              {span_window}
            GROUP BY trace_id
        )
        WHERE user_id != ''
        """
        return query, params

    def resolve_user_ids(self, trace_ids: list[str], analytics) -> dict[str, str]:
        """Resolve user_id strings for a page of trace IDs.

        Single-query lookup using ClickHouse enduser_dict:
        - Queries ClickHouse for user_id strings via dictionary lookup (~50-100ms)
        - No PostgreSQL round-trip needed

        Args:
            trace_ids: List of trace ID strings to resolve users for.
            analytics: Analytics service instance for executing CH queries.

        Returns:
            Dict mapping trace_id → user_id string.
        """
        if not trace_ids:
            return {}

        user_query, user_params = self.build_user_id_query(trace_ids)
        if not user_query:
            return {}

        result = analytics.execute_ch_query(user_query, user_params, timeout_ms=10000)

        # Build trace_id → user_id mapping (filter already applied in query)
        user_id_map = {
            str(row.get("trace_id", "")): row.get("user_id")
            for row in result.data
            if row.get("user_id")
        }

        return user_id_map

    @staticmethod
    def pivot_annotation_results(
        annotation_rows: list[dict],
        label_types: dict[str, str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Pivot annotation results keyed by trace_id.

        Returns:
            ``{trace_id: {label_id: annotation_value}}``.
        """
        import json

        label_types = label_types or {}
        result: dict[str, dict[str, Any]] = {}
        for row in annotation_rows:
            trace_id = str(row.get("trace_id", ""))
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

            result.setdefault(trace_id, {})[label_id] = value

        return result

    # ------------------------------------------------------------------
    # Result merging
    # ------------------------------------------------------------------

    @staticmethod
    def pivot_eval_results(
        eval_rows: list[tuple],
        eval_columns: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Pivot eval query results into a nested dict keyed by trace_id.

        Args:
            eval_rows: Rows from the Phase-2 eval query.
            eval_columns: Column names for those rows.

        Returns:
            A dict of ``{trace_id: {eval_config_id: score_dict}}``.
        """
        result: dict[str, dict[str, Any]] = {}
        col_idx = {name: i for i, name in enumerate(eval_columns)}

        def _get(row, key, idx, default=None):
            if isinstance(row, dict):
                return row.get(key, default)
            return (
                row[col_idx.get(key, idx)]
                if len(row) > col_idx.get(key, idx)
                else default
            )

        import json as _json

        for row in eval_rows:
            trace_id = str(_get(row, "trace_id", 0, ""))
            config_id = str(_get(row, "eval_config_id", 1, ""))
            avg_score = _get(row, "avg_score", 2)
            pass_rate = _get(row, "pass_rate", 3)
            success_count = _get(row, "success_count", 4, 0) or 0
            error_count = _get(row, "error_count", 5, 0) or 0
            str_lists = _get(row, "str_lists", 7, []) or []

            # All rows errored — surface an explicit error marker so the
            # UI can render an error state (distinct from "no eval run").
            if success_count == 0 and error_count > 0:
                result.setdefault(trace_id, {})[config_id] = {"error": True}
                continue

            # CHOICES eval: compute per-choice percentage across all
            # non-errored eval rows for this (trace, config) pair. Caller
            # spreads into ``{config_id}**{choice}`` columns.
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
                result.setdefault(trace_id, {})[config_id] = {
                    "per_choice": per_choice,
                }
                continue

            # ClickHouse ``avgIf`` returns NaN when no rows pass the
            # condition (or when all matching values are NULL). Python's
            # ``bool(float('nan'))`` is True, so a plain ``if avg_score``
            # guard leaks NaN into the JSON response and trips DRF's
            # strict encoder. Filter non-finite values explicitly.
            def _finite(v):
                return (
                    isinstance(v, (int, float))
                    and not isinstance(v, bool)
                    and math.isfinite(v)
                )

            avg_val = round(avg_score * 100, 2) if _finite(avg_score) else None
            pass_val = round(pass_rate, 2) if _finite(pass_rate) else None

            # No completed score: surface a non-terminal / skipped lifecycle
            # marker (skipped > running > pending) so the cell renders a
            # loading/pending/skipped state instead of a misleading blank.
            if avg_val is None and pass_val is None:
                marker = non_terminal_eval_marker(
                    {
                        "skipped_count": _get(row, "skipped_count", 8, 0) or 0,
                        "running_count": _get(row, "running_count", 9, 0) or 0,
                        "pending_count": _get(row, "pending_count", 10, 0) or 0,
                        "skipped_reason": _get(row, "skipped_reason", 11, None),
                    }
                )
                if marker is not None:
                    result.setdefault(trace_id, {})[config_id] = marker
                    continue

            score_data = {
                "avg_score": avg_val,
                "pass_rate": pass_val,
                "count": _get(row, "eval_count", 6, 0) or 0,
            }
            result.setdefault(trace_id, {})[config_id] = score_data

        return result
