"""Bounded, newest-first ClickHouse reads for filtered trace/span pages."""

from __future__ import annotations

import json
from collections.abc import Hashable
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import ceil
from time import monotonic
from typing import Any, Protocol

from tracer.services.clickhouse.query_service import QueryResult
from tracer.services.clickhouse.read_budget import is_read_budget_error

_INITIAL_SLICE = timedelta(minutes=5)
_MAX_SLICE = timedelta(days=2)
_MAX_SEED_ATTEMPTS = 24
_MAX_CANDIDATES = 512
_ABSOLUTE_MAX_CANDIDATES = 512
_ABSOLUTE_MAX_QUERIES = 128
_SELECTIVE_ANCHOR_SENTINEL = 513
_MAX_OPTIONAL_ANCHOR_STRATA = 4
# Keep one slow-but-bounded statement within the client deadline so the next
# seed/classifier can run. Production showed successful reads
# at 0.79-1.26 s under load; the caller's wall deadline, query count, rows,
# bytes, memory, and single-thread settings remain the authoritative envelope.
_QUERY_TIMEOUT_MS = 1_500
# Trace/span list queries fetch one additional page-sized de-duplication
# margin; 5,000 is also the existing server-side result ceiling used by those
# endpoints.  Keeping one public ceiling makes numbered-page work finite for
# filtered selectors, unfiltered top-K reads, and session OFFSET reads alike.
MAX_NUMBERED_PAGE_WORK_ROWS = 5_000
_READ_SETTINGS = {
    "max_threads": 1,
    "max_block_size": 8192,
    "max_memory_usage": 256 * 1024 * 1024,
    "max_bytes_to_read": 512 * 1024 * 1024,
    "read_overflow_mode": "throw",
    "max_result_rows": _MAX_CANDIDATES,
    "result_overflow_mode": "throw",
}

PAGE_DEPTH_EXCEEDED_CODE = "page_depth_exceeded"
PAGE_DEPTH_EXCEEDED_MESSAGE = (
    "The requested page is beyond the supported numbered-page depth. "
    "Request an earlier page or narrow the filter time range."
)


class FilterPageBuilder(Protocol):
    def parse_time_range(
        self, filters: list[dict[str, Any]]
    ) -> tuple[datetime, datetime]: ...

    def build_filter_seed_page(
        self,
        *,
        slice_start: datetime,
        slice_end: datetime,
        limit: int,
        before_start_time: datetime | None = None,
        before_id: Any = None,
    ) -> tuple[str, dict[str, Any]]: ...

    def build_filter_match_query(
        self, candidate_ids: list[str]
    ) -> tuple[str, dict[str, Any]]: ...


class QueryExecutor(Protocol):
    def execute_ch_query(
        self,
        query: str,
        params: dict[str, Any],
        *,
        timeout_ms: int,
        settings: dict[str, Any],
    ) -> QueryResult: ...


@dataclass(frozen=True)
class FilterReadAttempt:
    kind: str
    slice_start: datetime
    slice_end: datetime
    elapsed_ms: float
    rows_returned: int
    result_payload_bytes: int
    query_count: int = 1
    error_code: str | None = None


@dataclass(frozen=True)
class BoundedFilterPage:
    rows: list[dict[str, Any]]
    has_more: bool
    complete: bool
    status: str
    error_code: str | None
    total_rows_lower_bound: int
    elapsed_ms: float
    query_count: int
    rows_returned: int
    result_payload_bytes: int
    attempts: tuple[FilterReadAttempt, ...]
    # Graph-only two-phase reads may acquire a finite identity superset now and
    # classify the union once later. Keep raw seeds out of ``rows`` so a caller
    # can never mistake an unclassified anchor for a proven match.
    deferred_candidate_rows: tuple[dict[str, Any], ...] = ()
    classification_deferred: bool = False


class _BudgetExceeded(Exception):
    def __init__(self, error_code: str):
        self.error_code = error_code
        super().__init__(error_code)


def degraded_bounded_filter_page(error_code: str) -> BoundedFilterPage:
    """Return a sanitized incomplete page without issuing a database read."""

    return BoundedFilterPage(
        rows=[],
        has_more=False,
        complete=False,
        status="degraded",
        error_code=error_code,
        total_rows_lower_bound=0,
        elapsed_ms=0.0,
        query_count=0,
        rows_returned=0,
        result_payload_bytes=0,
        attempts=(),
    )


def numbered_page_depth_exceeded(
    *,
    page_number: int,
    page_size: int,
) -> bool:
    """Return whether numbered-page work exceeds the shared finite ceiling.

    Trace and span lists read a stable ordered prefix through the requested
    page plus one page-sized de-duplication margin. Session and filtered reads
    need no more work than that conservative bound, so one calculation safely
    gates every numbered-list path without changing membership below it.
    """

    if page_number < 0 or page_size <= 0:
        raise ValueError("page_number must be non-negative and page_size positive")
    return ((page_number + 2) * page_size) > MAX_NUMBERED_PAGE_WORK_ROWS


def bounded_numbered_page_depth_exceeded(
    *,
    page_number: int,
    page_size: int,
    max_seed_attempts: int = _MAX_SEED_ATTEMPTS,
    max_candidates: int = _MAX_CANDIDATES,
    max_query_count: int | None = None,
    classify_batch_size: int = 200,
    seed_batch_size: int = 200,
    reserved_query_count: int = 0,
) -> bool:
    """Return whether page N cannot fit inside the finite selector contract.

    This is a mechanical prefix-budget check only: it performs no database
    read and makes no claim that an accepted page will contain data.  Keeping
    the calculation public lets list transports reject unsupported deep
    numbered pages as a stable, non-retryable client error before ClickHouse is
    contacted.  Cursor/unbounded pagination is intentionally not implied.
    """

    if page_number < 0 or page_size <= 0:
        raise ValueError("page_number must be non-negative and page_size positive")
    if not 1 <= max_seed_attempts <= _ABSOLUTE_MAX_QUERIES:
        raise ValueError("max_seed_attempts exceeds the bounded read contract")
    if not 1 <= max_candidates <= _ABSOLUTE_MAX_CANDIDATES:
        raise ValueError("max_candidates exceeds the bounded read contract")
    if max_query_count is None:
        max_query_count = min(max_seed_attempts * 2, _ABSOLUTE_MAX_QUERIES)
    if not 1 <= max_query_count <= _ABSOLUTE_MAX_QUERIES:
        raise ValueError("max_query_count exceeds the bounded read contract")
    if not 0 <= reserved_query_count <= max_query_count:
        raise ValueError("reserved_query_count exceeds max_query_count")
    if not 1 <= classify_batch_size <= max_candidates:
        raise ValueError("classify_batch_size exceeds max_candidates")
    if not 1 <= seed_batch_size <= max_candidates:
        raise ValueError("seed_batch_size exceeds max_candidates")
    prefix_needed = ((page_number + 1) * page_size) + 1
    candidate_limit = min(max_candidates, max(seed_batch_size, prefix_needed))
    minimum_query_count = (
        reserved_query_count
        + ceil(prefix_needed / candidate_limit)
        + ceil(prefix_needed / classify_batch_size)
    )
    return (
        prefix_needed > max_seed_attempts * max_candidates
        or minimum_query_count > max_query_count
    )


def _without_timezone(value: datetime) -> datetime:
    return value.replace(tzinfo=None) if value.tzinfo is not None else value


def _result_payload_bytes(rows: list[dict[str, Any]]) -> int:
    return len(
        json.dumps(rows, default=str, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    )


def read_bounded_filter_page(
    *,
    builder: FilterPageBuilder,
    analytics: QueryExecutor,
    filters: list[dict[str, Any]],
    key_field: str,
    page_number: int,
    page_size: int,
    deadline_ms: int = 1_800,
    max_seed_attempts: int = _MAX_SEED_ATTEMPTS,
    max_candidates: int = _MAX_CANDIDATES,
    max_query_count: int | None = None,
    classify_batch_size: int | None = None,
    retry_wide_read_budget: bool = False,
    include_incomplete_rows: bool = False,
    cursor_start_time: datetime | None = None,
    cursor_order_token: Any = None,
    read_settings: dict[str, Any] | None = None,
    anchor_probe_only: bool = False,
    anchor_probe_limit: int | None = None,
    defer_classification: bool = False,
) -> BoundedFilterPage:
    """Return one exact numbered page or an explicit sanitized degradation.

    Seed reads cover adjacent half-open time slices in descending order. Each
    seed is only an identity/order superset; every ID is reclassified against
    global latest state before it can enter the page. A failed read is never
    retried as control flow, and a partial prefix is never exposed as page N.
    Graph callers may opt into proven-but-incomplete page-zero rows; numbered
    list/eval callers retain the exact/empty default.
    ``anchor_probe_only`` stops after a selective-anchor sentinel instead of
    entering the ordered seed loop; it is reserved for callers that provide a
    separate bounded fallback for non-sparse values.
    ``anchor_probe_limit`` lowers that sentinel for a caller whose surrounding
    protocol already partitions the complete request window. It is graph-only
    in practice: short-window numbered pages retain the 513-row sparse/common
    proof, while long-window trace builders may opt directly into ordered root
    batches. A graph stratum can classify its visible rows plus one finite
    sentinel without sorting the stratum's full match set.
    ``defer_classification`` is an internal graph-only acquisition contract. It
    returns finite seeds in ``deferred_candidate_rows`` and always leaves public
    ``rows`` empty; the caller must replay the union through the same builder's
    latest-state classifier before exposing any result.
    """

    if page_number < 0 or page_size <= 0 or deadline_ms <= 0:
        raise ValueError("page_number, page_size and deadline_ms must be positive")
    if (cursor_start_time is None) != (cursor_order_token is None):
        raise ValueError("cursor order values must be provided together")
    if cursor_start_time is not None and page_number != 0:
        raise ValueError("cursor reads must use page_number zero")
    if include_incomplete_rows and page_number != 0:
        raise ValueError("incomplete rows are available only for page zero")
    if defer_classification and (page_number != 0 or not include_incomplete_rows):
        raise ValueError(
            "deferred classification requires graph page-zero incomplete rows"
        )
    if not 1 <= max_seed_attempts <= _ABSOLUTE_MAX_QUERIES:
        raise ValueError("max_seed_attempts exceeds the bounded read contract")
    if not 1 <= max_candidates <= _ABSOLUTE_MAX_CANDIDATES:
        raise ValueError("max_candidates exceeds the bounded read contract")
    if anchor_probe_limit is not None:
        if not anchor_probe_only:
            raise ValueError("anchor_probe_limit requires anchor_probe_only")
        if not 2 <= anchor_probe_limit <= max_candidates:
            raise ValueError("anchor_probe_limit exceeds max_candidates")
        if page_size >= anchor_probe_limit:
            raise ValueError("anchor_probe_limit must include a page sentinel")
    if defer_classification:
        bounded_anchor_acquisition = (
            anchor_probe_only and anchor_probe_limit is not None
        )
        bounded_fallback_acquisition = (
            not anchor_probe_only
            and max_seed_attempts == 1
            and max_candidates <= 50
            and page_size < max_candidates
        )
        if not (bounded_anchor_acquisition or bounded_fallback_acquisition):
            raise ValueError(
                "deferred classification requires one bounded graph acquisition"
            )
    if max_query_count is None:
        max_query_count = min(max_seed_attempts * 2, _ABSOLUTE_MAX_QUERIES)
    if not 1 <= max_query_count <= _ABSOLUTE_MAX_QUERIES:
        raise ValueError("max_query_count exceeds the bounded read contract")

    recommended_batch_size: int | None = None
    batch_recommendation = getattr(
        builder, "recommended_filter_classify_batch_size", None
    )
    if callable(batch_recommendation):
        raw_recommendation = batch_recommendation()
        if raw_recommendation is not None:
            recommended_batch_size = int(raw_recommendation)
            if recommended_batch_size <= 0:
                raise ValueError("recommended classify batch size must be positive")
            recommended_batch_size = min(recommended_batch_size, max_candidates)
    if classify_batch_size is None:
        classify_batch_size = recommended_batch_size or min(200, max_candidates)
    if not 1 <= classify_batch_size <= max_candidates:
        raise ValueError("classify_batch_size exceeds max_candidates")

    started = monotonic()
    deadline = started + (deadline_ms / 1000)
    seed_order_proof = getattr(builder, "filter_seed_proves_result_order", None)
    seed_proves_result_order = (
        bool(seed_order_proof()) if callable(seed_order_proof) else True
    )
    request_start, request_end = builder.parse_time_range(filters)
    request_start = _without_timezone(request_start)
    request_end = _without_timezone(request_end)
    cursor_key: tuple[datetime, Any] | None = None
    if cursor_start_time is not None:
        cursor_key = (_without_timezone(cursor_start_time), cursor_order_token)

    # Resolve the optional anchor plan once, before the numbered-page preflight,
    # and reuse it at execution time.  The probe is speculative: when it reaches
    # its sentinel the ordered seed/classifier path still has to run, so that
    # path must reserve the probe's physical query up front.
    anchor_builder = getattr(builder, "build_filter_anchor_probe", None)
    anchor_support = getattr(builder, "supports_filter_anchor_probe", None)
    ordered_seed_builder = getattr(builder, "build_filter_ordered_seed_page", None)
    recommended_anchor_limit: int | None = None
    recommended_anchor_timeout_ms: int | None = None
    recommended_anchor_strata = 1
    recommended_anchor_max_bytes_to_read: int | None = None
    if anchor_probe_limit is None and not anchor_probe_only:
        anchor_limit_builder = getattr(
            builder, "recommended_filter_anchor_probe_limit", None
        )
        if callable(anchor_limit_builder):
            raw_anchor_limit = anchor_limit_builder()
            if raw_anchor_limit is not None:
                recommended_anchor_limit = int(raw_anchor_limit)
                if not 2 <= recommended_anchor_limit <= max_candidates:
                    raise ValueError(
                        "recommended anchor probe limit exceeds max_candidates"
                    )
                anchor_timeout_builder = getattr(
                    builder, "recommended_filter_anchor_probe_timeout_ms", None
                )
                if callable(anchor_timeout_builder):
                    raw_anchor_timeout = anchor_timeout_builder()
                    if raw_anchor_timeout is not None:
                        recommended_anchor_timeout_ms = int(raw_anchor_timeout)
                        if recommended_anchor_timeout_ms <= 0:
                            raise ValueError(
                                "recommended anchor probe timeout must be positive"
                            )
                anchor_strata_builder = getattr(
                    builder, "recommended_filter_anchor_probe_strata", None
                )
                if callable(anchor_strata_builder):
                    raw_anchor_strata = anchor_strata_builder()
                    if raw_anchor_strata is not None:
                        recommended_anchor_strata = int(raw_anchor_strata)
                        if (
                            not 1
                            <= recommended_anchor_strata
                            <= _MAX_OPTIONAL_ANCHOR_STRATA
                        ):
                            raise ValueError(
                                "recommended anchor probe strata exceeds bounded contract"
                            )
                anchor_bytes_builder = getattr(
                    builder,
                    "recommended_filter_anchor_probe_max_bytes_to_read",
                    None,
                )
                if callable(anchor_bytes_builder):
                    raw_anchor_bytes = anchor_bytes_builder()
                    if raw_anchor_bytes is not None:
                        recommended_anchor_max_bytes_to_read = int(raw_anchor_bytes)
                        if not (
                            0
                            < recommended_anchor_max_bytes_to_read
                            < _READ_SETTINGS["max_bytes_to_read"]
                        ):
                            raise ValueError(
                                "recommended anchor byte cap must tighten the read contract"
                            )
    anchor_limit = (
        anchor_probe_limit
        or recommended_anchor_limit
        or min(
            _SELECTIVE_ANCHOR_SENTINEL,
            max_candidates + 1,
        )
    )
    skip_full_anchor_builder = getattr(
        builder,
        "skip_full_window_filter_anchor_probe",
        None,
    )
    skip_full_window_anchor = (
        anchor_limit == _SELECTIVE_ANCHOR_SENTINEL
        and anchor_probe_limit is None
        and not anchor_probe_only
        and callable(skip_full_anchor_builder)
        and bool(skip_full_anchor_builder())
    )
    anchor_can_run = (
        cursor_key is None
        and (
            anchor_limit == _SELECTIVE_ANCHOR_SENTINEL
            or anchor_probe_limit is not None
            or recommended_anchor_limit is not None
        )
        and callable(anchor_builder)
        and callable(anchor_support)
        and bool(anchor_support())
        and not skip_full_window_anchor
        and (callable(ordered_seed_builder) or seed_proves_result_order)
    )

    cursor_seed_keyset_capability = getattr(
        builder, "filter_cursor_seed_keyset_is_safe", None
    )
    cursor_seed_keyset_is_safe = (
        bool(cursor_seed_keyset_capability())
        if callable(cursor_seed_keyset_capability)
        else True
    )
    if (
        cursor_key is not None
        and not cursor_seed_keyset_is_safe
        and not callable(ordered_seed_builder)
    ):
        raise ValueError("unsafe cursor seeds require an ordered seed builder")
    if request_start >= request_end:
        return BoundedFilterPage(
            rows=[],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=0,
            elapsed_ms=(monotonic() - started) * 1000,
            query_count=0,
            rows_returned=0,
            result_payload_bytes=0,
            attempts=(),
        )

    prefix_needed = ((page_number + 1) * page_size) + 1
    # A builder can lower both the seed and classifier working set when its
    # candidate query is memory-heavy. The requested prefix remains a floor so
    # page zero includes its has-more sentinel in the first ordered root seed.
    seed_batch_recommendation = getattr(
        builder, "recommended_filter_seed_batch_size", None
    )
    candidate_floor = min(
        int(seed_batch_recommendation())
        if callable(seed_batch_recommendation)
        else recommended_batch_size or min(200, max_candidates),
        max_candidates,
    )
    if not 1 <= candidate_floor <= max_candidates:
        raise ValueError("recommended seed batch size exceeds max_candidates")
    candidate_limit = min(max_candidates, max(candidate_floor, prefix_needed))
    page_depth_kwargs = {
        "page_number": page_number,
        "page_size": page_size,
        "max_seed_attempts": max_seed_attempts,
        "max_candidates": max_candidates,
        "max_query_count": max_query_count,
        "classify_batch_size": classify_batch_size,
        "seed_batch_size": candidate_floor,
    }
    if bounded_numbered_page_depth_exceeded(
        **page_depth_kwargs,
    ):
        return BoundedFilterPage(
            rows=[],
            has_more=False,
            complete=False,
            status="degraded",
            error_code=PAGE_DEPTH_EXCEEDED_CODE,
            total_rows_lower_bound=0,
            elapsed_ms=(monotonic() - started) * 1000,
            query_count=0,
            rows_returned=0,
            result_payload_bytes=0,
            attempts=(),
        )
    if (
        anchor_can_run
        and not anchor_probe_only
        and (
            recommended_anchor_strata > max_query_count
            or bounded_numbered_page_depth_exceeded(
                **page_depth_kwargs,
                reserved_query_count=recommended_anchor_strata,
            )
        )
    ):
        # The optional probe may fall back after its last physical stratum.
        # If the fallback cannot retain its complete budget, skip speculation
        # before contacting ClickHouse.
        anchor_can_run = False

    attempts: list[FilterReadAttempt] = []
    # A trace any-span seed is a physical *span*, while the classified result
    # identity is its trace.  Keep these namespaces separate: using trace_id as
    # the seed key both breaks keyset continuation when one trace has multiple
    # matching spans and can stop a dense selective scan before it is proven
    # exhaustive.
    seen_seed_ids: set[Hashable] = set()
    seen_candidate_ids: set[Hashable] = set()
    matched_by_id: dict[Hashable, dict[str, Any]] = {}
    deferred_candidate_by_id: dict[Hashable, dict[str, Any]] = {}
    if cursor_key is not None and cursor_key[0] < request_start:
        return BoundedFilterPage(
            rows=[],
            has_more=False,
            complete=True,
            status="complete",
            error_code=None,
            total_rows_lower_bound=0,
            elapsed_ms=(monotonic() - started) * 1000,
            query_count=0,
            rows_returned=0,
            result_payload_bytes=0,
            attempts=(),
        )
    # Safe seed/result orders may start directly at the signed cursor. Trace
    # raw roots are only an upper bound on the canonical live-root order: a
    # tombstoned newer root can expose an older alternate root. Those cursors
    # therefore restart at the frozen request end, then apply the signed public
    # boundary after latest-state classification below.
    use_cursor_seed_keyset = cursor_key is not None and cursor_seed_keyset_is_safe
    slice_end = (
        min(request_end, cursor_key[0] + timedelta(microseconds=1))
        if use_cursor_seed_keyset
        else request_end
    )
    slice_width = _INITIAL_SLICE
    before_start_time: datetime | None = (
        cursor_key[0] if use_cursor_seed_keyset else None
    )
    before_id: Any = cursor_key[1] if use_cursor_seed_keyset else None
    forced_width_cap: timedelta | None = None
    page_complete = False
    degraded_error_code: str | None = None

    def execute(
        *,
        kind: str,
        query: str,
        params: dict[str, Any],
        active_start: datetime,
        active_end: datetime,
        result_limit: int = _ABSOLUTE_MAX_CANDIDATES,
        timeout_cap_ms: int | None = None,
        max_bytes_to_read_cap: int | None = None,
    ) -> QueryResult:
        remaining_ms = int((deadline - monotonic()) * 1000)
        if remaining_ms < 25:
            raise _BudgetExceeded("deadline_exceeded")
        if len(attempts) >= max_query_count:
            raise _BudgetExceeded("query_budget_exceeded")
        attempt_started = monotonic()
        statement_timeout_ms = min(_QUERY_TIMEOUT_MS, remaining_ms)
        if timeout_cap_ms is not None:
            if timeout_cap_ms <= 0:
                raise ValueError("query timeout cap must be positive")
            statement_timeout_ms = min(statement_timeout_ms, timeout_cap_ms)
        try:
            settings = {
                **_READ_SETTINGS,
                **(read_settings or {}),
                "max_result_rows": result_limit,
            }
            if max_bytes_to_read_cap is not None:
                if max_bytes_to_read_cap <= 0:
                    raise ValueError("query byte cap must be positive")
                settings["max_bytes_to_read"] = min(
                    int(settings["max_bytes_to_read"]),
                    max_bytes_to_read_cap,
                )
            result = analytics.execute_ch_query(
                query,
                params,
                timeout_ms=statement_timeout_ms,
                settings=settings,
            )
        except Exception as exc:
            if not is_read_budget_error(exc):
                raise
            attempts.append(
                FilterReadAttempt(
                    kind=kind,
                    slice_start=active_start,
                    slice_end=active_end,
                    elapsed_ms=(monotonic() - attempt_started) * 1000,
                    rows_returned=0,
                    result_payload_bytes=0,
                    error_code="read_budget_exceeded",
                )
            )
            raise _BudgetExceeded("read_budget_exceeded") from None
        rows = list(result.data or [])
        attempts.append(
            FilterReadAttempt(
                kind=kind,
                slice_start=active_start,
                slice_end=active_end,
                elapsed_ms=(monotonic() - attempt_started) * 1000,
                rows_returned=len(rows),
                result_payload_bytes=_result_payload_bytes(rows),
            )
        )
        return result

    def row_identity(row: dict[str, Any]) -> Hashable:
        identity_builder = getattr(builder, "bounded_filter_row_identity", None)
        identity: Hashable = (
            identity_builder(row)
            if callable(identity_builder)
            else str(row.get(key_field, ""))
        )
        try:
            hash(identity)
        except TypeError as exc:
            raise ValueError("bounded filter row identity must be hashable") from exc
        return identity

    def result_row_key(row: dict[str, Any]) -> tuple[datetime, Any]:
        value = row.get("start_time")
        start_time = (
            _without_timezone(value) if isinstance(value, datetime) else datetime.min
        )
        order_builder = getattr(builder, "bounded_filter_row_order_token", None)
        order_token = (
            order_builder(row)
            if callable(order_builder)
            else str(row.get(key_field, ""))
        )
        return start_time, order_token

    def seed_identity(row: dict[str, Any]) -> Hashable:
        identity_builder = getattr(builder, "bounded_filter_seed_identity", None)
        identity: Hashable = (
            identity_builder(row) if callable(identity_builder) else row_identity(row)
        )
        try:
            hash(identity)
        except TypeError as exc:
            raise ValueError("bounded filter seed identity must be hashable") from exc
        return identity

    def seed_row_key(row: dict[str, Any]) -> tuple[datetime, Any]:
        value = row.get("start_time")
        start_time = (
            _without_timezone(value) if isinstance(value, datetime) else datetime.min
        )
        order_builder = getattr(builder, "bounded_filter_seed_order_token", None)
        if callable(order_builder):
            return start_time, order_builder(row)
        return result_row_key(row)

    def classify_seed_rows(
        candidate_rows: list[dict[str, Any]],
        *,
        active_start: datetime,
        active_end: datetime,
        stop_on_ordered_prefix: bool = False,
    ) -> bool:
        """Classify finite seeds and report an exact ordered-prefix proof.

        Root-ordered seed rows are an upper bound on the canonical live-root
        order for their trace: latest-state tombstones can move a classified
        result older, but cannot move it ahead of that trace's newest raw live
        root seed.  After each finite classifier chunk, a full public prefix
        whose cutoff is no older than the last classified raw seed therefore
        cannot be displaced by any remaining seed in the same ordered page.

        Callers must leave ``stop_on_ordered_prefix`` false for unordered
        any-span anchors, direct child seeds, and deferred graph acquisition.
        """

        candidate_seed_rows: dict[Hashable, dict[str, Any]] = {}
        for row in candidate_rows:
            public_identity = str(row.get(key_field, ""))
            if not public_identity:
                continue
            candidate_identity = row_identity(row)
            if candidate_identity in seen_candidate_ids:
                continue
            seen_candidate_ids.add(candidate_identity)
            candidate_seed_rows[candidate_identity] = row

        candidate_identities = list(candidate_seed_rows)
        if defer_classification:
            deferred_candidate_by_id.update(candidate_seed_rows)
            return False
        for batch_offset in range(0, len(candidate_identities), classify_batch_size):
            identity_batch = candidate_identities[
                batch_offset : batch_offset + classify_batch_size
            ]
            candidate_batch = [
                str(candidate_seed_rows[identity].get(key_field, ""))
                for identity in identity_batch
            ]
            seeded_match_builder = getattr(
                builder,
                "build_filter_match_query_from_seed_rows",
                None,
            )
            if callable(seeded_match_builder):
                match_query, match_params = seeded_match_builder(
                    [candidate_seed_rows[identity] for identity in identity_batch]
                )
            else:
                match_query, match_params = builder.build_filter_match_query(
                    candidate_batch
                )
            if not match_query:
                continue
            match_result = execute(
                kind="classify",
                query=match_query,
                params=match_params,
                active_start=active_start,
                active_end=active_end,
                result_limit=max_candidates,
            )
            for row in match_result.data:
                identity = row_identity(row)
                # A classifier can return an updated ordering value different
                # from its raw seed. Apply the signed result boundary again so
                # continuation pages remain strictly disjoint.
                if cursor_key is not None and result_row_key(row) >= cursor_key:
                    continue
                if str(row.get(key_field, "")):
                    matched_by_id[identity] = row

            if stop_on_ordered_prefix and len(matched_by_id) >= prefix_needed:
                ordered_matches = sorted(
                    matched_by_id.values(), key=result_row_key, reverse=True
                )
                cutoff = result_row_key(ordered_matches[prefix_needed - 1])
                last_classified_seed = candidate_seed_rows[identity_batch[-1]]
                if cutoff >= seed_row_key(last_classified_seed):
                    return True
        return False

    try:
        # Eligible any-span trace filters first ask a direct key+value predicate
        # for a finite DISTINCT trace-id sentinel. If it exhausts, that is an
        # exact candidate superset and no root-history scan is needed. Long
        # windows may skip that broad sentinel and start with ordered roots. If
        # an attempted probe reaches the sentinel (or its read budget), switch
        # to ordered root batches, where a proven page prefix can close without
        # materialising the tenant-wide set that triggered Code 159.
        use_seed_loop = True
        seed_page_builder = builder.build_filter_seed_page
        if cursor_key is not None:
            # Continuations must start from the signed *result* tuple. Trace
            # any-span seeds are ordered by the matching physical child, not
            # by the public root trace, so use the root-ordered fallback when
            # the builder exposes it. Span seeds already share result order.
            if callable(ordered_seed_builder):
                seed_page_builder = ordered_seed_builder
                seed_proves_result_order = True
        elif anchor_can_run:
            try:
                anchor_rows_by_id: dict[Hashable, dict[str, Any]] = {}
                anchor_hit_sentinel = False
                if recommended_anchor_strata > 1 and not anchor_probe_only:
                    duration = request_end - request_start
                    duration_us = (
                        duration.days * 86_400_000_000
                        + duration.seconds * 1_000_000
                        + duration.microseconds
                    )
                    boundaries = [
                        request_start
                        + timedelta(
                            microseconds=(duration_us * index)
                            // recommended_anchor_strata
                        )
                        for index in range(recommended_anchor_strata + 1)
                    ]
                    boundaries[0], boundaries[-1] = request_start, request_end
                    anchor_slices = list(
                        reversed(
                            list(
                                zip(
                                    boundaries[:-1],
                                    boundaries[1:],
                                    strict=False,
                                )
                            )
                        )
                    )
                else:
                    anchor_slices = [(request_start, request_end)]

                for anchor_start, anchor_end in anchor_slices:
                    remaining_anchor_limit = anchor_limit - len(anchor_rows_by_id)
                    if remaining_anchor_limit <= 0:
                        anchor_hit_sentinel = True
                        break
                    if recommended_anchor_strata > 1 and not anchor_probe_only:
                        anchor_query, anchor_params = anchor_builder(
                            limit=remaining_anchor_limit,
                            slice_start=anchor_start,
                            slice_end=anchor_end,
                        )
                    else:
                        anchor_query, anchor_params = anchor_builder(
                            limit=remaining_anchor_limit
                        )
                    anchor_result = execute(
                        kind="anchor",
                        query=anchor_query,
                        params=anchor_params,
                        active_start=anchor_start,
                        active_end=anchor_end,
                        result_limit=remaining_anchor_limit,
                        timeout_cap_ms=recommended_anchor_timeout_ms,
                        max_bytes_to_read_cap=(
                            recommended_anchor_max_bytes_to_read
                            if recommended_anchor_strata > 1 and not anchor_probe_only
                            else None
                        ),
                    )
                    stratum_rows = list(anchor_result.data or [])
                    if len(stratum_rows) >= remaining_anchor_limit:
                        anchor_hit_sentinel = True
                        break
                    for row in stratum_rows:
                        anchor_rows_by_id[row_identity(row)] = row

                anchor_rows = [
                    row
                    for _, row in sorted(
                        anchor_rows_by_id.items(),
                        key=lambda item: repr(item[0]),
                    )
                ]
                if not anchor_hit_sentinel:
                    classify_seed_rows(
                        anchor_rows,
                        active_start=request_start,
                        active_end=request_end,
                    )
                    page_complete = True
                    use_seed_loop = False
                elif anchor_probe_only:
                    # The sentinel proves only that this value is not sparse.
                    # A partitioned graph probe classifies only this finite
                    # sentinel set and exposes at most the requested page.  It
                    # never falls into the ORDER BY seed loop; the surrounding
                    # stratum protocol supplies bounded temporal coverage and
                    # retains explicit sampled metadata.
                    if anchor_probe_limit is not None:
                        classify_seed_rows(
                            list(anchor_result.data or []),
                            active_start=request_start,
                            active_end=request_end,
                        )
                    degraded_error_code = "sample_limit"
                    use_seed_loop = False
                else:
                    if callable(ordered_seed_builder):
                        seed_page_builder = ordered_seed_builder
                        seed_proves_result_order = True
            except _BudgetExceeded as exc:
                if exc.error_code != "read_budget_exceeded":
                    raise
                if anchor_probe_only:
                    degraded_error_code = exc.error_code
                    use_seed_loop = False
                elif callable(ordered_seed_builder):
                    seed_page_builder = ordered_seed_builder
                    seed_proves_result_order = True
        elif (
            callable(ordered_seed_builder)
            and callable(anchor_support)
            and bool(anchor_support())
        ):
            # A caller-imposed working set smaller than the 512+1 sparse/common
            # sentinel cannot safely run the anchor probe. Fall straight back
            # to finite root-ordered batches; their order proof closes page N
            # without an extra full-slice exhaustion read.
            seed_page_builder = ordered_seed_builder
            seed_proves_result_order = True
        elif callable(ordered_seed_builder) and not seed_proves_result_order:
            # Some any-span predicates (notably structured JSON/call_type)
            # have no selective index. A whole-window anchor probe would be
            # the broad scan this bounded reader exists to avoid, so start
            # directly with finite newest-root batches and classify only those
            # candidate trace IDs.
            seed_page_builder = ordered_seed_builder
            seed_proves_result_order = True

        for seed_index in range(max_seed_attempts if use_seed_loop else 0):
            if slice_end <= request_start:
                page_complete = True
                break

            remaining_attempts = max_seed_attempts - seed_index
            remaining_window = slice_end - request_start
            scheduled_width = slice_width
            scheduled_coverage = timedelta(0)
            for _ in range(remaining_attempts):
                scheduled_coverage += scheduled_width
                scheduled_width = min(scheduled_width * 2, _MAX_SLICE)
            active_width = slice_width
            if scheduled_coverage < remaining_window:
                active_width = max(active_width, remaining_window / remaining_attempts)
            if forced_width_cap is not None:
                active_width = min(active_width, forced_width_cap)
            slice_start = max(request_start, slice_end - active_width)

            seed_query, seed_params = seed_page_builder(
                slice_start=slice_start,
                slice_end=slice_end,
                limit=candidate_limit,
                before_start_time=before_start_time,
                before_id=before_id,
            )
            try:
                seed_result = execute(
                    kind="seed",
                    query=seed_query,
                    params=seed_params,
                    active_start=slice_start,
                    active_end=slice_end,
                )
            except _BudgetExceeded as exc:
                if (
                    retry_wide_read_budget
                    and exc.error_code == "read_budget_exceeded"
                    and active_width > _INITIAL_SLICE
                ):
                    forced_width_cap = max(_INITIAL_SLICE, active_width / 2)
                    before_start_time = None
                    before_id = None
                    continue
                raise
            forced_width_cap = None
            seed_rows = sorted(seed_result.data, key=seed_row_key, reverse=True)
            new_candidate_rows: list[dict[str, Any]] = []
            for row in seed_rows:
                public_identity = str(row.get(key_field, ""))
                raw_identity = seed_identity(row)
                if not public_identity or raw_identity in seen_seed_ids:
                    continue
                seen_seed_ids.add(raw_identity)
                new_candidate_rows.append(row)

            prefix_is_proven = classify_seed_rows(
                new_candidate_rows,
                active_start=slice_start,
                active_end=slice_end,
                stop_on_ordered_prefix=seed_proves_result_order,
            )

            if prefix_is_proven:
                page_complete = True
                break

            slice_exhausted = len(seed_rows) < candidate_limit
            ordered_matches = sorted(
                matched_by_id.values(), key=result_row_key, reverse=True
            )
            if seed_proves_result_order and len(ordered_matches) >= prefix_needed:
                cutoff = result_row_key(ordered_matches[prefix_needed - 1])
                prefix_is_proven = (
                    cutoff[0] >= slice_start
                    if slice_exhausted
                    else cutoff >= seed_row_key(seed_rows[-1])
                )
                if prefix_is_proven:
                    page_complete = True
                    break

            if not slice_exhausted:
                if not seed_rows:
                    break
                next_start_time, next_id = seed_row_key(seed_rows[-1])
                if (next_start_time, next_id) == (before_start_time, before_id):
                    break
                before_start_time, before_id = next_start_time, next_id
                continue

            if slice_start <= request_start:
                page_complete = True
                break
            slice_end = slice_start
            slice_width = min(active_width * 2, _MAX_SLICE)
            before_start_time = None
            before_id = None
    except _BudgetExceeded as exc:
        page_complete = False
        degraded_error_code = exc.error_code

    ordered_matches = sorted(matched_by_id.values(), key=result_row_key, reverse=True)
    offset = page_number * page_size
    has_more = len(ordered_matches) > offset + page_size
    page_rows = (
        ordered_matches[offset : offset + page_size]
        if page_complete or include_incomplete_rows
        else []
    )
    error_code = (
        None if page_complete else degraded_error_code or "scan_budget_exceeded"
    )
    return BoundedFilterPage(
        # Raw seeds are not latest-state matches. Even graph callers that opt
        # into incomplete rows may expose only the outer union classifier's
        # result, never this acquisition phase.
        rows=[] if defer_classification else page_rows,
        has_more=has_more if page_complete or include_incomplete_rows else False,
        complete=page_complete,
        status="complete" if page_complete else "degraded",
        error_code=error_code,
        total_rows_lower_bound=len(ordered_matches),
        elapsed_ms=(monotonic() - started) * 1000,
        query_count=len(attempts),
        rows_returned=sum(attempt.rows_returned for attempt in attempts),
        result_payload_bytes=sum(attempt.result_payload_bytes for attempt in attempts),
        attempts=tuple(attempts),
        deferred_candidate_rows=tuple(deferred_candidate_by_id.values()),
        classification_deferred=defer_classification,
    )


__all__ = [
    "BoundedFilterPage",
    "FilterReadAttempt",
    "PAGE_DEPTH_EXCEEDED_CODE",
    "PAGE_DEPTH_EXCEEDED_MESSAGE",
    "MAX_NUMBERED_PAGE_WORK_ROWS",
    "bounded_numbered_page_depth_exceeded",
    "degraded_bounded_filter_page",
    "numbered_page_depth_exceeded",
    "read_bounded_filter_page",
]
