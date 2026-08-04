"""Bounded ClickHouse 25.3 selectors for span attribute picker APIs.

The picker surfaces in this module are discovery aids, not accounting reads.
They walk a fixed one-year horizon in adjacent half-open bands, cap every
physical read, and replay every selected physical span identity through
``argMax(_version)`` before accepting a key or value.  That keeps tombstones
and cleared attributes from leaking stale data even when span ids are reused.

Only the CH25 ``spans`` table is read.  Callers must perform their PostgreSQL
project ownership check before constructing the selector; telemetry never
falls back to PostgreSQL.
"""

from __future__ import annotations

import json
import math
import threading
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import structlog

from tracer.services.clickhouse.client import ClickHouseClient
from tracer.services.clickhouse.read_budget import (
    ReadDeadlineExceeded,
    is_read_budget_error,
)
from tracer.utils.filter_operators import (
    JSON_ARRAY_FILTER_MAX_STRING_UTF8_BYTES,
    JSON_ARRAY_FILTER_MAX_TOTAL_STRING_UTF8_BYTES,
)

logger = structlog.get_logger(__name__)

AttributeType = Literal["string", "number", "boolean", "array", "json"]
JsonAttributeMode = Literal["none", "scalars", "arrays", "all"]
QueryStatus = Literal["complete", "sampled", "degraded"]
PhysicalSpanIdentity = tuple[str, str, str, datetime]
JsonScalar = str | int | float | bool
AttributeValue = str | int | float | bool | tuple[JsonScalar, ...]

ATTRIBUTE_READ_HORIZON_DAYS = (7, 14, 30, 180, 365)
# Allow a candidate read and its mandatory latest-state replay to each receive
# their independent 1.5 s server budget even when the client is briefly
# descheduled. Row, byte, result, and per-query ceilings remain unchanged.
ATTRIBUTE_READ_WALL_TIMEOUT_MS = 6_000
ATTRIBUTE_READ_QUERY_TIMEOUT_MS = 1_500
# JSON overflow has no key skip index. Keep its independent lane short so a
# rare/absent JSON key cannot consume the whole picker deadline after typed Map
# rows have already been verified.
ATTRIBUTE_READ_JSON_QUERY_TIMEOUT_MS = 750
ATTRIBUTE_READ_EXPLICIT_SEGMENT = timedelta(days=1)
# Keep each storage-order seed small enough that dense projects stop inside the
# read envelope before ClickHouse pulls another large attribute block.  The
# extra row requested by ``_candidate_ids`` remains an explicit truncation
# sentinel, so callers never mistake this discovery sample for a complete
# distribution.
ATTRIBUTE_READ_CANDIDATE_LIMIT = 64
# Value pickers replay full typed Map values only after acquiring a finite
# identity set. Keep that acquisition deliberately tiny so a key lookup cannot
# pull another large values block before LIMIT stops the read.
ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT = 8
# Exact-key discovery may continue through a small number of deterministic
# candidate pages when a storage-order first probe replays entirely to
# cleared/tombstoned latest state. This cap is shared across adaptive bands and
# lanes; it never turns generic key inventory into an open-ended scan.
ATTRIBUTE_READ_TARGETED_CANDIDATE_PAGE_LIMIT = 6
# A stale-only value probe may use this many deterministic continuation pages.
# First probes cover all adaptive bands before these pages run round-robin, so a
# dense recent week cannot hide an older value. A first sample that already has
# usable values remains a visibly degraded sample instead of paying for a full
# global sort. The six-second wall deadline remains the tighter production cap.
ATTRIBUTE_READ_VALUE_CANDIDATE_PAGE_LIMIT = 6
ATTRIBUTE_READ_VALUE_TOTAL_CANDIDATE_PAGE_LIMIT = 15
ATTRIBUTE_READ_MAX_KEYS = 1_000
ATTRIBUTE_READ_MAX_VALUES = 500
ATTRIBUTE_READ_MAX_KEY_BYTES = 512
ATTRIBUTE_READ_MAX_SEARCH_BYTES = 512
ATTRIBUTE_READ_MAX_PROJECTS = 64

_MIB = 1024 * 1024
ATTRIBUTE_READ_SETTINGS: dict[str, Any] = {
    "max_threads": 1,
    # None of the current spans projections covers the identity plus every
    # typed attribute-key subcolumn used by these selectors.  Letting CH25
    # consider all of them adds material planning time before a bounded read
    # can start, while never producing a usable plan for this query shape.
    "optimize_use_projections": 0,
    "allow_experimental_projection_optimization": 0,
    # A small block lets LIMIT BY stop dense candidate scans promptly instead
    # of pulling the default ~65k-row block after the 513th identity.
    "max_block_size": 8_192,
    "max_memory_usage": 256 * _MIB,
    "max_bytes_to_read": 512 * _MIB,
    "max_rows_to_read": 500_000,
    "read_overflow_mode": "throw",
    "max_result_rows": ATTRIBUTE_READ_CANDIDATE_LIMIT + 1,
    "max_result_bytes": 16 * _MIB,
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
}

_TYPE_PRIORITY: dict[AttributeType, int] = {
    "string": 0,
    "number": 1,
    "boolean": 2,
    "array": 3,
    "json": 4,
}


class InvalidAttributeKey(ValueError):
    """A requested attribute key is not safe for the public picker API."""


class InvalidAttributeSearch(ValueError):
    """A requested value-search term is not safe for the public picker API."""


class IncompleteLatestStateReplay(RuntimeError):
    """A candidate set could not be fully verified at its latest state."""


@dataclass(frozen=True)
class AttributeQueryPage:
    data: list[dict[str, Any]]
    query_time_ms: float


@dataclass(frozen=True)
class AttributeReadMetadata:
    query_complete: bool
    query_status: QueryStatus
    query_error_code: str | None
    query_window_start: datetime
    query_window_end: datetime
    query_count: int

    def public_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "query_complete": self.query_complete,
            "query_status": self.query_status,
            "query_window_start": _utc_iso(self.query_window_start),
            "query_window_end": _utc_iso(self.query_window_end),
        }
        if self.query_error_code:
            payload["query_error_code"] = self.query_error_code
        return payload


@dataclass(frozen=True)
class AttributeKeyRow:
    key: str
    type: AttributeType
    count: int


@dataclass(frozen=True)
class AttributeValueRow:
    value: AttributeValue
    type: AttributeType
    count: int


@dataclass(frozen=True)
class AttributeKeyRead:
    rows: tuple[AttributeKeyRow, ...]
    metadata: AttributeReadMetadata


@dataclass(frozen=True)
class AttributeValueRead:
    rows: tuple[AttributeValueRow, ...]
    metadata: AttributeReadMetadata


@dataclass(frozen=True)
class AttributeDetailRead:
    """Bounded latest-state value sample for one attribute's detail panel."""

    attribute_type: AttributeType | None
    rows: tuple[AttributeValueRow, ...]
    metadata: AttributeReadMetadata


@dataclass(frozen=True)
class AttributeCardinalityRead:
    max_spans_per_trace: int
    max_traces_per_session: int
    metadata: AttributeReadMetadata


class AttributeKeyInventory(list):
    """List-compatible typed inventory with explicit bounded-read metadata."""

    def __init__(self, read: AttributeKeyRead, *, include_counts: bool = False):
        super().__init__(
            {
                "key": row.key,
                "type": row.type,
                **({"count": row.count} if include_counts else {}),
            }
            for row in read.rows
        )
        self.query_complete = read.metadata.query_complete
        self.query_status = read.metadata.query_status
        self.query_error_code = read.metadata.query_error_code
        self.query_window_start = read.metadata.query_window_start
        self.query_window_end = read.metadata.query_window_end
        self.query_count = read.metadata.query_count


def _validate_text(
    value: Any,
    *,
    label: str,
    max_bytes: int,
    allow_empty: bool,
    error_type: type[ValueError],
) -> str:
    if not isinstance(value, str):
        raise error_type(f"{label} must be text")
    if not allow_empty and not value.strip():
        raise error_type(f"{label} is required")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise error_type(f"{label} contains control characters")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise error_type(f"{label} must be valid UTF-8") from exc
    if len(encoded) > max_bytes:
        raise error_type(f"{label} is too long")
    return value


def validate_attribute_key(value: Any) -> str:
    """Validate without restricting punctuation or non-ASCII key names."""

    return _validate_text(
        value,
        label="Attribute key",
        max_bytes=ATTRIBUTE_READ_MAX_KEY_BYTES,
        allow_empty=False,
        error_type=InvalidAttributeKey,
    )


def validate_attribute_search(value: Any) -> str:
    """Validate a literal UTF-8 contains-search term."""

    return _validate_text(
        value,
        label="Attribute search",
        max_bytes=ATTRIBUTE_READ_MAX_SEARCH_BYTES,
        allow_empty=True,
        error_type=InvalidAttributeSearch,
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _utc_iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _unix_microseconds(value: datetime) -> int:
    """Encode DateTime64(6) exactly without driver tuple-datetime truncation."""

    delta = _utc(value) - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def adaptive_attribute_windows(
    window_end: datetime,
    *,
    horizon_days: int = 365,
) -> tuple[tuple[datetime, datetime], ...]:
    """Return newest-first adjacent 7d/14d/30d/6mo/1yr bands."""

    if horizon_days < 1 or horizon_days > ATTRIBUTE_READ_HORIZON_DAYS[-1]:
        raise ValueError("horizon_days must be between 1 and 365")
    end = _utc(window_end)
    boundaries = [day for day in ATTRIBUTE_READ_HORIZON_DAYS if day < horizon_days]
    boundaries.append(horizon_days)
    windows: list[tuple[datetime, datetime]] = []
    previous = 0
    for boundary in boundaries:
        windows.append((end - timedelta(days=boundary), end - timedelta(days=previous)))
        previous = boundary
    return tuple(windows)


class V2AttributeQueryExecutor:
    """Read-only native-driver executor bound explicitly to ``CLICKHOUSE_V2``."""

    def __init__(self, client: ClickHouseClient | None = None):
        if client is None:
            # Lazy to avoid a query_service -> attribute_reads import cycle.
            from tracer.services.clickhouse.v2.query_service import (
                get_v2_query_client,
            )

            client = get_v2_query_client()
        self._client = client

    @property
    def client(self) -> ClickHouseClient:
        return self._client

    def execute(
        self,
        query: str,
        params: dict[str, Any],
        *,
        timeout_ms: int,
        settings: dict[str, Any],
    ) -> AttributeQueryPage:
        rows, columns, query_time_ms = self._client.execute_read(
            query,
            params,
            timeout_ms=timeout_ms,
            settings=settings,
        )
        names = [
            column[0] if isinstance(column, tuple) else column for column in columns
        ]
        return AttributeQueryPage(
            data=[dict(zip(names, row, strict=False)) for row in rows],
            query_time_ms=float(query_time_ms),
        )


_ATTRIBUTE_READ_CAPACITY = threading.BoundedSemaphore(8)


# The first probe follows the spans sorting key and has no LIMIT BY. Picker reads
# are samples, not chronological lists: the former newest-first global sort and
# LIMIT BY forced CH25 to process every matching row before returning the first
# candidate on large tenants. ``optimize_read_in_order`` lets this storage-order
# LIMIT stop as soon as a finite page is available. Raw duplicate versions may
# consume sample slots; the +1 sentinel then marks the response incomplete, and
# every retained physical identity is still replayed through argMax(_version)
# before use. Thus background merges may change which *sample* is returned, but
# can never turn a sampled response into a false exact response.
_CANDIDATE_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(trace_id) AS trace_id,
        toString(id) AS id,
        start_time
    FROM spans AS attribute_source
    PREWHERE project_id IN %(project_ids)s
      AND start_time >= %(segment_start)s
      AND start_time < %(segment_end)s
    WHERE is_deleted = 0
      AND ({candidate_predicate})
    ORDER BY
        attribute_source.project_id ASC,
        attribute_source.observation_type ASC,
        attribute_source.service_name ASC,
        toStartOfHour(attribute_source.start_time) ASC,
        attribute_source.trace_id ASC,
        attribute_source.id ASC
    LIMIT %(candidate_limit)s
"""

_STRATIFIED_CANDIDATE_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(trace_id) AS trace_id,
        toString(id) AS id,
        start_time,
        toUInt64(1) AS sample_size
    FROM spans AS attribute_source
    PREWHERE project_id IN %(project_ids)s
      AND start_time >= %(segment_start)s
      AND start_time < %(segment_end)s
    WHERE is_deleted = 0
      AND ({candidate_predicate})
    ORDER BY
        attribute_source.project_id ASC,
        attribute_source.observation_type ASC,
        attribute_source.service_name ASC,
        toStartOfHour(attribute_source.start_time) ASC,
        attribute_source.trace_id ASC,
        attribute_source.id ASC
    LIMIT %(candidate_limit)s
"""

# Targeted discovery/value reads may encounter a first storage-order sample made
# entirely of stale versions whose latest state cleared the requested key. Only
# that case restarts with this deterministic keyset query. Generic browse and
# successful targeted probes never pay the global ordering cost.
_ORDERED_CANDIDATE_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(trace_id) AS trace_id,
        toString(id) AS id,
        start_time
    FROM spans AS attribute_source
    PREWHERE project_id IN %(project_ids)s
      AND start_time >= %(segment_start)s
      AND start_time < %(segment_end)s
    WHERE is_deleted = 0
      AND ({candidate_predicate})
    ORDER BY
        start_time DESC,
        id DESC,
        trace_id DESC,
        toString(attribute_source.project_id) DESC
    LIMIT 1 BY project_id, trace_id, id, start_time
    LIMIT %(candidate_limit)s
"""

_ORDERED_STRATIFIED_CANDIDATE_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(trace_id) AS trace_id,
        toString(id) AS id,
        start_time,
        toUInt64(1) AS sample_size
    FROM spans AS attribute_source
    PREWHERE project_id IN %(project_ids)s
      AND start_time >= %(segment_start)s
      AND start_time < %(segment_end)s
    WHERE is_deleted = 0
      AND ({candidate_predicate})
    ORDER BY
        start_time DESC,
        id DESC,
        trace_id DESC,
        toString(attribute_source.project_id) DESC
    LIMIT 1 BY project_id, trace_id, id, start_time
    LIMIT %(candidate_limit)s
"""

_LATEST_TARGET_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(id) AS id,
        tupleElement(latest_state, 1) AS start_time,
        tupleElement(latest_state, 2) AS is_deleted,
        tupleElement(latest_state, 3) AS trace_id,
        tupleElement(latest_state, 4) AS trace_session_id,
        tupleElement(latest_state, 5) AS parent_span_id,
        tupleElement(latest_state, 6) AS string_present,
        tupleElement(latest_state, 7) AS string_value,
        tupleElement(latest_state, 8) AS number_present,
        tupleElement(latest_state, 9) AS number_value,
        tupleElement(latest_state, 10) AS boolean_present,
        tupleElement(latest_state, 11) AS boolean_value,
        tupleElement(latest_state, 12) AS legacy_present,
        tupleElement(latest_state, 13) AS legacy_value_raw
    FROM
    (
        SELECT
            project_id,
            id,
            argMax(
                tuple(
                    start_time,
                    is_deleted,
                    trace_id,
                    ifNull(toString(trace_session_id), ''),
                    parent_span_id,
                    mapContains(attrs_string, %(attribute_key)s),
                    attrs_string[%(attribute_key)s],
                    mapContains(attrs_number, %(attribute_key)s),
                    attrs_number[%(attribute_key)s],
                    mapContains(attrs_bool, %(attribute_key)s),
                    attrs_bool[%(attribute_key)s],
                    JSONHas(attributes_extra, %(attribute_key)s),
                    JSONExtractRaw(attributes_extra, %(attribute_key)s)
                ),
                _version
            ) AS latest_state
        FROM spans AS attribute_source
        PREWHERE project_id IN %(project_ids)s
          AND ({candidate_predicate})
        GROUP BY project_id, trace_id, id, start_time
    )
"""

_LATEST_TYPED_TARGET_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(trace_id) AS trace_id,
        toString(id) AS id,
        start_time,
        tupleElement(latest_state, 1) AS is_deleted,
        tupleElement(latest_state, 2) AS string_present,
        tupleElement(latest_state, 3) AS string_value,
        tupleElement(latest_state, 4) AS number_present,
        tupleElement(latest_state, 5) AS number_value,
        tupleElement(latest_state, 6) AS boolean_present,
        tupleElement(latest_state, 7) AS boolean_value
    FROM
    (
        SELECT
            project_id,
            trace_id,
            id,
            start_time,
            argMax(
                tuple(
                    is_deleted,
                    mapContains(attrs_string, %(attribute_key)s),
                    attrs_string[%(attribute_key)s],
                    mapContains(attrs_number, %(attribute_key)s),
                    attrs_number[%(attribute_key)s],
                    mapContains(attrs_bool, %(attribute_key)s),
                    attrs_bool[%(attribute_key)s]
                ),
                _version
            ) AS latest_state
        FROM spans AS attribute_source
        PREWHERE project_id IN %(project_ids)s
          AND ({candidate_predicate})
        GROUP BY project_id, trace_id, id, start_time
    )
"""

_LATEST_BROWSE_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(id) AS id,
        tupleElement(latest_state, 1) AS start_time,
        tupleElement(latest_state, 2) AS is_deleted,
        tupleElement(latest_state, 3) AS trace_id,
        tupleElement(latest_state, 4) AS trace_session_id,
        tupleElement(latest_state, 5) AS parent_span_id,
        tupleElement(latest_state, 6) AS string_keys,
        tupleElement(latest_state, 7) AS number_keys,
        tupleElement(latest_state, 8) AS boolean_keys,
        tupleElement(latest_state, 9) AS attributes_extra
    FROM
    (
        SELECT
            project_id,
            id,
            argMax(
                tuple(
                    start_time,
                    is_deleted,
                    trace_id,
                    ifNull(toString(trace_session_id), ''),
                    parent_span_id,
                    attrs_string.keys,
                    attrs_number.keys,
                    attrs_bool.keys,
                    attributes_extra
                ),
                _version
            ) AS latest_state
        FROM spans AS attribute_source
        PREWHERE project_id IN %(project_ids)s
          AND ({candidate_predicate})
        GROUP BY project_id, trace_id, id, start_time
    )
"""

_LATEST_TYPED_BROWSE_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(trace_id) AS trace_id,
        toString(id) AS id,
        start_time,
        tupleElement(latest_state, 1) AS is_deleted,
        tupleElement(latest_state, 2) AS string_keys,
        tupleElement(latest_state, 3) AS number_keys,
        tupleElement(latest_state, 4) AS boolean_keys
    FROM
    (
        SELECT
            project_id,
            trace_id,
            id,
            start_time,
            argMax(
                tuple(
                    is_deleted,
                    attrs_string.keys,
                    attrs_number.keys,
                    attrs_bool.keys
                ),
                _version
            ) AS latest_state
        FROM spans AS attribute_source
        PREWHERE project_id IN %(project_ids)s
          AND ({candidate_predicate})
        GROUP BY project_id, trace_id, id, start_time
    )
"""

_LATEST_CARDINALITY_SQL = """
    SELECT
        toString(project_id) AS project_id,
        toString(id) AS id,
        tupleElement(latest_state, 1) AS start_time,
        tupleElement(latest_state, 2) AS is_deleted,
        tupleElement(latest_state, 3) AS trace_id,
        tupleElement(latest_state, 4) AS trace_session_id
    FROM
    (
        SELECT
            project_id,
            id,
            argMax(
                tuple(
                    start_time,
                    is_deleted,
                    trace_id,
                    ifNull(toString(trace_session_id), '')
                ),
                _version
            ) AS latest_state
        FROM spans AS attribute_source
        PREWHERE project_id IN %(project_ids)s
          AND ({candidate_predicate})
        GROUP BY project_id, trace_id, id, start_time
    )
"""


class AttributeReadSelector:
    """Thin typed selector shared by every production attribute picker.

    Each public operation gets one six-second wall budget shared by all of its
    adaptive candidate and latest-state replay queries. Default-horizon reads
    keep the existing finite band/page caps; caller-supplied windows are split
    into adjacent day probes under the same whole-operation deadline. Common
    dense typed reads stop after one candidate/replay pair and explicitly
    report a sample. Reusing a
    selector for a second public operation starts a fresh operation budget;
    per-query caps remain 1.5 s.
    """

    def __init__(
        self,
        executor: V2AttributeQueryExecutor | None = None,
        *,
        now: datetime | None = None,
        wall_timeout_ms: int = ATTRIBUTE_READ_WALL_TIMEOUT_MS,
        clock: Callable[[], float] = time.monotonic,
        typed_only: bool = False,
        json_attribute_mode: JsonAttributeMode | None = None,
    ):
        self._executor = executor or V2AttributeQueryExecutor()
        self._clock = clock
        self._wall_timeout_seconds = max(int(wall_timeout_ms), 1) / 1000
        self._deadline: float | None = None
        self._window_end = _utc(now or datetime.now(UTC))
        self._query_count = 0
        # ``typed_only`` remains the compatibility switch for callers that
        # must never touch the JSON overflow.  Filter pickers opt into
        # ``arrays`` explicitly: structured array predicates are supported by
        # the bounded classifier, while JSON-only scalars/objects are not and
        # therefore must not be advertised as filterable.  Eval mapping uses
        # ``all`` because it needs key names, not a filter operator contract.
        self._typed_only = bool(typed_only)
        if json_attribute_mode is None:
            json_attribute_mode = "none" if self._typed_only else "scalars"
        if json_attribute_mode not in {"none", "scalars", "arrays", "all"}:
            raise ValueError("Unsupported JSON attribute discovery mode")
        self._json_attribute_mode: JsonAttributeMode = json_attribute_mode
        self._reads_json_overflow = json_attribute_mode != "none"

    @property
    def executor(self) -> V2AttributeQueryExecutor:
        return self._executor

    @property
    def query_count(self) -> int:
        return self._query_count

    @property
    def query_window_end(self) -> datetime:
        return self._window_end

    def degraded_metadata(self, error_code: str) -> AttributeReadMetadata:
        """Build a sanitized failure envelope for a discarded read."""

        return self._metadata(
            complete=False,
            error_code=error_code,
            window_start=self._window_end
            - timedelta(days=ATTRIBUTE_READ_HORIZON_DAYS[-1]),
            window_end=self._window_end,
            query_count=self._query_count,
        )

    def _warn_partial_budget(self, operation: str) -> None:
        """Record intentional partial retention without leaking query details."""

        logger.warning(
            "attribute_read_partial_budget_exceeded",
            operation=operation,
            query_count=self._query_count,
        )

    def _begin_operation(self) -> None:
        """Start a fresh whole-operation budget at the public call boundary."""

        self._deadline = self._clock() + self._wall_timeout_seconds
        self._query_count = 0

    def _execute(
        self,
        query: str,
        params: dict[str, Any],
        *,
        max_result_rows: int,
        query_settings: dict[str, Any] | None = None,
        query_timeout_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        if self._deadline is None:
            self._begin_operation()
        assert self._deadline is not None
        remaining_ms = int((self._deadline - self._clock()) * 1000)
        if remaining_ms < 25:
            raise ReadDeadlineExceeded("Attribute read deadline exceeded")
        acquired = _ATTRIBUTE_READ_CAPACITY.acquire(
            timeout=max(self._deadline - self._clock(), 0)
        )
        if not acquired:
            raise ReadDeadlineExceeded("Attribute read capacity is busy")
        try:
            remaining_ms = int((self._deadline - self._clock()) * 1000)
            if remaining_ms < 25:
                raise ReadDeadlineExceeded("Attribute read deadline exceeded")
            self._query_count += 1
            timeout_cap_ms = (
                ATTRIBUTE_READ_QUERY_TIMEOUT_MS
                if query_timeout_ms is None
                else min(max(int(query_timeout_ms), 1), ATTRIBUTE_READ_QUERY_TIMEOUT_MS)
            )
            page = self._executor.execute(
                query,
                params,
                timeout_ms=min(timeout_cap_ms, remaining_ms),
                settings={
                    **ATTRIBUTE_READ_SETTINGS,
                    **(query_settings or {}),
                    "max_result_rows": max(int(max_result_rows), 1),
                },
            )
        finally:
            _ATTRIBUTE_READ_CAPACITY.release()
        if not isinstance(page, AttributeQueryPage) or not isinstance(page.data, list):
            raise IncompleteLatestStateReplay(
                "Attribute query returned an invalid result envelope"
            )
        return page.data

    def _windows(
        self,
        *,
        horizon_days: int,
        window_start: datetime | None,
        window_end: datetime | None,
    ) -> tuple[tuple[datetime, datetime], ...]:
        if (window_start is None) != (window_end is None):
            raise ValueError("window_start and window_end must be provided together")
        if window_start is not None and window_end is not None:
            start = _utc(window_start)
            end = _utc(window_end)
            if start >= end:
                raise ValueError("window_start must be before window_end")
            # Explicit dashboard/eval windows can be dense even at seven days.
            # Walk adjacent newest-first day slices so a single picker probe
            # cannot turn the entire requested range into one physical scan.
            windows: list[tuple[datetime, datetime]] = []
            segment_end = end
            while segment_end > start:
                segment_start = max(
                    start, segment_end - ATTRIBUTE_READ_EXPLICIT_SEGMENT
                )
                windows.append((segment_start, segment_end))
                segment_end = segment_start
            return tuple(windows)
        return adaptive_attribute_windows(
            self._window_end,
            horizon_days=horizon_days,
        )

    @staticmethod
    def _project_ids(project_ids: Iterable[Any]) -> tuple[str, ...]:
        projects: list[str] = []
        for project_id in project_ids:
            if not project_id:
                continue
            try:
                canonical = str(uuid.UUID(str(project_id)))
            except (TypeError, ValueError, AttributeError) as exc:
                raise IncompleteLatestStateReplay(
                    "Attribute read received an invalid project identity"
                ) from exc
            if canonical not in projects:
                projects.append(canonical)
            if len(projects) > ATTRIBUTE_READ_MAX_PROJECTS:
                raise IncompleteLatestStateReplay(
                    "Attribute read project scope exceeds its hard cap"
                )
        return tuple(projects)

    @staticmethod
    def _candidate_pair_predicate(
        candidate_ids: tuple[PhysicalSpanIdentity, ...],
    ) -> tuple[str, dict[str, Any]]:
        """Compile a finite, fully parameterized physical replay predicate.

        A direct-write span is identified by project, trace, id and start time;
        span ids are only trace-unique. Keeping raw ``id``/``trace_id`` IN
        predicates alongside exact partition dates and an integer-microsecond
        tuple lets ClickHouse use both bloom indexes and partition pruning,
        without allowing a tombstone from another physical span to win. Integer
        microseconds avoid clickhouse-driver truncating DateTime64 values inside
        tuple parameters. Only generated parameter names enter SQL; values
        remain driver-bound.
        """

        identities_by_project: dict[str, list[tuple[str, str, datetime]]] = defaultdict(
            list
        )
        for project_id, trace_id, candidate_id, start_time in candidate_ids:
            identities_by_project[project_id].append(
                (trace_id, candidate_id, start_time)
            )

        clauses: list[str] = []
        params: dict[str, Any] = {}
        for index, (project_id, identities) in enumerate(identities_by_project.items()):
            project_param = f"candidate_project_{index}"
            ids_param = f"candidate_ids_{index}"
            trace_ids_param = f"candidate_trace_ids_{index}"
            dates_param = f"candidate_dates_{index}"
            identities_param = f"candidate_physical_identities_{index}"
            span_ids = tuple(dict.fromkeys(item[1] for item in identities))
            trace_ids = tuple(dict.fromkeys(item[0] for item in identities))
            dates = tuple(dict.fromkeys(item[2].date() for item in identities))
            encoded_identities = tuple(
                (trace_id, span_id, _unix_microseconds(start_time))
                for trace_id, span_id, start_time in identities
            )
            clauses.append(
                f"(project_id = toUUID(%({project_param})s) "
                f"AND id IN %({ids_param})s "
                f"AND trace_id IN %({trace_ids_param})s "
                f"AND toDate(start_time) IN %({dates_param})s "
                "AND (trace_id, id, toUnixTimestamp64Micro(start_time)) "
                f"IN %({identities_param})s)"
            )
            params[project_param] = project_id
            params[ids_param] = span_ids
            params[trace_ids_param] = trace_ids
            params[dates_param] = dates
            params[identities_param] = encoded_identities
        if not clauses:
            raise IncompleteLatestStateReplay(
                "Attribute latest-state replay had no candidate identities"
            )
        return " OR ".join(clauses), params

    @staticmethod
    def _single_project_scope_sql(
        sql: str,
        project_ids: tuple[str, ...],
        params: dict[str, Any],
    ) -> str:
        """Avoid CH25 Set/index planning for the normal one-project API case."""

        if len(project_ids) != 1:
            return sql
        params["scope_project_id"] = project_ids[0]
        return sql.replace(
            "project_id IN %(project_ids)s",
            "attribute_source.project_id = toUUID(%(scope_project_id)s)",
        )

    def _candidate_ids(
        self,
        project_ids: tuple[str, ...],
        segment: tuple[datetime, datetime],
        *,
        predicate: str,
        attribute_key: str | None,
        attribute_search: str | None = None,
        stratified: bool = False,
        ordered: bool = False,
        before_identity: PhysicalSpanIdentity | None = None,
        candidate_limit: int,
        query_timeout_ms: int | None = None,
    ) -> tuple[tuple[PhysicalSpanIdentity, ...], bool]:
        segment_start, segment_end = segment
        params: dict[str, Any] = {
            "project_ids": project_ids,
            "segment_start": segment_start,
            "segment_end": segment_end,
            "candidate_limit": candidate_limit + 1,
        }
        if attribute_key is not None:
            params["attribute_key"] = attribute_key
        if attribute_search is not None:
            params["attribute_search"] = attribute_search
        ordered = ordered or before_identity is not None
        candidate_sql = _ORDERED_CANDIDATE_SQL if ordered else _CANDIDATE_SQL
        query_settings: dict[str, Any] = {}
        if stratified:
            candidate_sql = (
                _ORDERED_STRATIFIED_CANDIDATE_SQL
                if ordered
                else _STRATIFIED_CANDIDATE_SQL
            )
            # Generic inventory predicates cannot use the Map-key bloom
            # indexes.  Disabling skip-index planning avoids building useless
            # index conditions; primary-key and partition pruning remain on.
            query_settings["use_skip_indexes"] = 0
        if not ordered:
            # The first finite probe is aligned exactly with the MergeTree
            # sorting-key prefix, so CH25 can stop without a global sort.
            query_settings["optimize_read_in_order"] = 1
        if before_identity is not None:
            before_project_id, before_trace_id, before_id, before_start_time = (
                before_identity
            )
            if (
                before_project_id not in project_ids
                or not segment_start <= before_start_time < segment_end
            ):
                raise ValueError("candidate keyset must stay inside its segment")
            params.update(
                {
                    "candidate_before_start_us": _unix_microseconds(before_start_time),
                    "candidate_before_id": before_id,
                    "candidate_before_trace_id": before_trace_id,
                    "candidate_before_project_id": before_project_id,
                }
            )
            predicate = (
                f"({predicate}) AND "
                "(toUnixTimestamp64Micro(start_time) "
                "< %(candidate_before_start_us)s "
                "OR (toUnixTimestamp64Micro(start_time) "
                "= %(candidate_before_start_us)s AND "
                "(id < %(candidate_before_id)s "
                "OR (id = %(candidate_before_id)s AND "
                "(trace_id < %(candidate_before_trace_id)s "
                "OR (trace_id = %(candidate_before_trace_id)s AND "
                "toString(attribute_source.project_id) "
                "< %(candidate_before_project_id)s))))))"
            )
        candidate_sql = self._single_project_scope_sql(
            candidate_sql, project_ids, params
        )
        rows = self._execute(
            candidate_sql.format(candidate_predicate=predicate),
            params,
            max_result_rows=candidate_limit + 1,
            query_settings=query_settings,
            query_timeout_ms=query_timeout_ms,
        )
        truncated = len(rows) > candidate_limit
        identities: list[PhysicalSpanIdentity] = []
        seen: set[PhysicalSpanIdentity] = set()
        for row in rows[:candidate_limit]:
            candidate_project_id = str(row.get("project_id") or "")
            candidate_trace_id = str(row.get("trace_id") or "")
            candidate_id = str(row.get("id") or "")
            candidate_start_time = row.get("start_time")
            if (
                not candidate_project_id
                or not candidate_id
                or not isinstance(candidate_start_time, datetime)
            ):
                raise IncompleteLatestStateReplay(
                    "Attribute candidate query returned an invalid identity"
                )
            identity = (
                candidate_project_id,
                candidate_trace_id,
                candidate_id,
                _utc(candidate_start_time),
            )
            if identity not in seen:
                seen.add(identity)
                identities.append(identity)
        return tuple(identities), truncated

    def _verify_latest(
        self,
        *,
        sql: str,
        project_ids: tuple[str, ...],
        candidate_ids: tuple[PhysicalSpanIdentity, ...],
        attribute_key: str | None = None,
        query_timeout_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        if not candidate_ids:
            return []
        candidate_predicate, candidate_params = self._candidate_pair_predicate(
            candidate_ids
        )
        params: dict[str, Any] = {
            "project_ids": project_ids,
            **candidate_params,
        }
        if attribute_key is not None:
            params["attribute_key"] = attribute_key
        replay_sql = self._single_project_scope_sql(
            sql.format(candidate_predicate=candidate_predicate),
            project_ids,
            params,
        )
        rows = self._execute(
            replay_sql,
            params,
            max_result_rows=len(candidate_ids),
            query_timeout_ms=query_timeout_ms,
        )
        returned_ids = {self._physical_identity(row) for row in rows}
        if returned_ids != set(candidate_ids):
            raise IncompleteLatestStateReplay(
                "Attribute candidate latest-state replay was incomplete"
            )
        return rows

    @staticmethod
    def _physical_identity(row: dict[str, Any]) -> PhysicalSpanIdentity:
        """Return the immutable identity used by direct-write span readers."""

        project_id = str(row.get("project_id") or "")
        trace_id = str(row.get("trace_id") or "")
        span_id = str(row.get("id") or "")
        start_time = row.get("start_time")
        if not project_id or not span_id or not isinstance(start_time, datetime):
            raise IncompleteLatestStateReplay(
                "Attribute latest-state replay omitted physical identity"
            )
        return project_id, trace_id, span_id, _utc(start_time)

    @staticmethod
    def _row_is_active_in_window(
        row: dict[str, Any],
        window_start: datetime,
        window_end: datetime,
    ) -> bool:
        start_time = row.get("start_time")
        if not isinstance(start_time, datetime):
            raise IncompleteLatestStateReplay(
                "Attribute latest-state replay omitted start_time"
            )
        return (
            int(row.get("is_deleted") or 0) == 0
            and window_start <= _utc(start_time) < window_end
        )

    @staticmethod
    def _decode_legacy_scalar(
        raw: Any, *, json_encoded: bool = True
    ) -> tuple[AttributeType, Any] | None:
        if raw in (None, ""):
            return None
        try:
            value = json.loads(raw) if json_encoded and isinstance(raw, str) else raw
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if isinstance(value, str):
            return "string", value
        if isinstance(value, bool):
            return "boolean", value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            numeric = float(value)
            if math.isfinite(numeric):
                return "number", numeric
        return None

    @classmethod
    def _decode_json_attribute(
        cls,
        raw: Any,
        *,
        mode: JsonAttributeMode,
        json_encoded: bool = True,
    ) -> tuple[AttributeType, Any] | None:
        """Decode only JSON value families the caller can faithfully use.

        ``arrays`` is the filter-picker contract.  It intentionally ignores
        JSON-only scalars and objects: the bounded list classifier supports
        array membership over ``attributes_extra`` but scalar filters still
        use the indexed typed Maps.  ``all`` is reserved for eval mapping,
        where an object/null key is a valid field path even though it is not a
        filterable scalar.  Array members are reduced to the exact finite JSON
        scalar vocabulary accepted by the public filter serializer.
        """

        if mode == "none" or raw == "" or (raw is None and json_encoded):
            return None
        try:
            value = json.loads(raw) if json_encoded and isinstance(raw, str) else raw
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

        if mode in {"scalars", "all"}:
            scalar = cls._decode_legacy_scalar(value, json_encoded=False)
            if scalar is not None:
                return scalar
        if mode in {"arrays", "all"} and isinstance(value, list):
            members: list[JsonScalar] = []
            seen: set[tuple[str, str]] = set()
            total_string_bytes = 0
            for member in value:
                if member is None or member == "":
                    continue
                if isinstance(member, bool):
                    canonical = ("boolean", "true" if member else "false")
                elif isinstance(member, str):
                    member_bytes = len(member.encode("utf-8"))
                    if member_bytes > JSON_ARRAY_FILTER_MAX_STRING_UTF8_BYTES:
                        continue
                    if (
                        total_string_bytes + member_bytes
                        > JSON_ARRAY_FILTER_MAX_TOTAL_STRING_UTF8_BYTES
                    ):
                        continue
                    total_string_bytes += member_bytes
                    canonical = (
                        "string",
                        json.dumps(member, ensure_ascii=False, separators=(",", ":")),
                    )
                elif isinstance(member, int):
                    if not (-(1 << 63) <= member <= (1 << 64) - 1):
                        continue
                    canonical = ("integer", str(member))
                elif isinstance(member, float) and math.isfinite(member):
                    canonical = (
                        "number",
                        json.dumps(member, allow_nan=False, separators=(",", ":")),
                    )
                else:
                    # Nested arrays/objects are deliberately not selectable;
                    # the backend rejects them instead of relying on JSON
                    # serialization order.
                    continue
                if canonical not in seen:
                    seen.add(canonical)
                    members.append(member)
                    if len(members) > ATTRIBUTE_READ_MAX_VALUES:
                        break
            return "array", tuple(members)
        if mode == "all":
            # Eval mapping only consumes the key/type, never this value.  A
            # single sentinel keeps null/object keys discoverable without
            # copying their potentially large structure into Python state.
            return "json", None
        return None

    @classmethod
    def _decode_target_value(
        cls,
        row: dict[str, Any],
        *,
        json_attribute_mode: JsonAttributeMode = "scalars",
    ) -> tuple[AttributeType, Any] | None:
        """Apply stable typed-Map precedence, then the requested JSON tier."""

        if bool(row.get("string_present")):
            value = row.get("string_value")
            return ("string", str(value)) if value is not None else None
        if bool(row.get("number_present")):
            value = row.get("number_value")
            if value is None:
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            return ("number", numeric) if math.isfinite(numeric) else None
        if bool(row.get("boolean_present")):
            value = row.get("boolean_value")
            return ("boolean", bool(value)) if value is not None else None
        if json_attribute_mode != "none" and bool(row.get("legacy_present")):
            return cls._decode_json_attribute(
                row.get("legacy_value_raw"),
                mode=json_attribute_mode,
            )
        return None

    @classmethod
    def _browse_row_keys(
        cls,
        row: dict[str, Any],
        *,
        json_attribute_mode: JsonAttributeMode = "scalars",
    ) -> tuple[dict[str, AttributeType], bool]:
        """Return keys supported by the caller's explicit JSON contract.

        Typed Maps always win when the same key also appears in overflow JSON.
        Legacy scalar mode retains its historical degraded signal when it sees
        a structured value; array-filter and eval-mapping modes intentionally
        define which additional JSON families are actionable.
        """
        keys: dict[str, AttributeType] = {}
        unsupported_value_seen = False
        for attr_type, field in (
            ("string", "string_keys"),
            ("number", "number_keys"),
            ("boolean", "boolean_keys"),
        ):
            raw_keys = row.get(field) or []
            if not isinstance(raw_keys, (tuple, list)):
                raise IncompleteLatestStateReplay(
                    "Attribute latest-state replay returned invalid Map keys"
                )
            for raw_key in raw_keys:
                key = str(raw_key)
                if key and key not in keys:
                    keys[key] = attr_type

        raw_extra = (
            row.get("attributes_extra") if json_attribute_mode != "none" else None
        )
        if raw_extra not in (None, "", "{}"):
            try:
                extra = (
                    json.loads(raw_extra) if isinstance(raw_extra, str) else raw_extra
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                extra = {}
            if isinstance(extra, dict):
                for raw_key, raw_value in extra.items():
                    key = str(raw_key)
                    if not key or key in keys:
                        continue
                    decoded = cls._decode_json_attribute(
                        raw_value,
                        mode=json_attribute_mode,
                        json_encoded=False,
                    )
                    if decoded is not None:
                        keys[key] = decoded[0]
                    elif json_attribute_mode == "scalars":
                        unsupported_value_seen = True
        return keys, unsupported_value_seen

    @classmethod
    def _target_value_is_unsupported(
        cls,
        row: dict[str, Any],
        *,
        json_attribute_mode: JsonAttributeMode,
    ) -> bool:
        """Whether the selected JSON contract saw a value it cannot type."""

        if any(
            bool(row.get(field))
            for field in ("string_present", "number_present", "boolean_present")
        ):
            return False
        if not bool(row.get("legacy_present")):
            return False
        decoded = cls._decode_json_attribute(
            row.get("legacy_value_raw"),
            mode=json_attribute_mode,
        )
        # ``arrays`` intentionally excludes scalar/object overflow values from
        # the filterable-key contract.  Only legacy scalar mode retains the
        # historical degraded signal for omitted structured values.
        return decoded is None and json_attribute_mode == "scalars"

    @staticmethod
    def _metadata(
        *,
        complete: bool,
        error_code: str | None,
        sampled: bool = False,
        window_start: datetime,
        window_end: datetime,
        query_count: int,
    ) -> AttributeReadMetadata:
        query_status: QueryStatus = "complete"
        if not complete:
            query_status = (
                "sampled" if sampled and error_code == "sample_limit" else "degraded"
            )
        return AttributeReadMetadata(
            query_complete=complete,
            query_status=query_status,
            query_error_code=error_code,
            query_window_start=window_start,
            query_window_end=window_end,
            query_count=query_count,
        )

    def discover_keys(
        self,
        project_ids: Iterable[Any],
        *,
        exact_key: str | None = None,
        horizon_days: int = 365,
        max_keys: int = ATTRIBUTE_READ_MAX_KEYS,
        order_by_count_desc: bool = False,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
    ) -> AttributeKeyRead:
        self._begin_operation()
        projects = self._project_ids(project_ids)
        if exact_key is not None:
            exact_key = validate_attribute_key(exact_key)
        max_keys = min(max(int(max_keys), 1), ATTRIBUTE_READ_MAX_KEYS)
        windows = self._windows(
            horizon_days=horizon_days,
            window_start=window_start,
            window_end=window_end,
        )
        overall_start, overall_end = windows[-1][0], windows[0][1]
        if not projects:
            return AttributeKeyRead(
                (),
                self._metadata(
                    complete=True,
                    error_code=None,
                    window_start=overall_start,
                    window_end=overall_end,
                    query_count=self._query_count,
                ),
            )

        typed_predicate = (
            "length(attrs_string.keys) > 0 "
            "OR length(attrs_number.keys) > 0 "
            "OR length(attrs_bool.keys) > 0"
            if exact_key is None
            else (
                "(indexHint(has(mapKeys(attrs_string), %(attribute_key)s)) "
                "AND has(attrs_string.keys, %(attribute_key)s)) "
                "OR (indexHint(has(mapKeys(attrs_number), %(attribute_key)s)) "
                "AND has(attrs_number.keys, %(attribute_key)s)) "
                "OR (indexHint(has(mapKeys(attrs_bool), %(attribute_key)s)) "
                "AND has(attrs_bool.keys, %(attribute_key)s))"
            )
        )
        json_predicate = (
            "attributes_extra NOT IN ('', '{}', 'null')"
            if exact_key is None
            else "JSONHas(attributes_extra, %(attribute_key)s)"
        )
        lanes: list[tuple[str, str, str, JsonAttributeMode, int | None]] = [
            (
                "typed",
                typed_predicate,
                _LATEST_TYPED_TARGET_SQL
                if exact_key is not None
                else _LATEST_TYPED_BROWSE_SQL,
                "none",
                None,
            )
        ]
        if self._reads_json_overflow:
            lanes.append(
                (
                    "json",
                    json_predicate,
                    _LATEST_TARGET_SQL if exact_key is not None else _LATEST_BROWSE_SQL,
                    self._json_attribute_mode,
                    ATTRIBUTE_READ_JSON_QUERY_TIMEOUT_MS,
                )
            )

        latest_keys: dict[PhysicalSpanIdentity, dict[str, AttributeType]] = {}
        truncated = False
        budget_exceeded = False
        json_budget_exceeded = False
        budget_warning_emitted = False
        covered_start = overall_end
        json_lane_available = self._reads_json_overflow
        typed_lane_halted = False

        def mark_budget_exceeded() -> None:
            nonlocal budget_exceeded, budget_warning_emitted
            budget_exceeded = True
            if not budget_warning_emitted:
                self._warn_partial_budget("discover_keys")
                budget_warning_emitted = True

        def mark_json_budget_exceeded() -> None:
            nonlocal json_budget_exceeded, budget_warning_emitted
            json_budget_exceeded = True
            if not budget_warning_emitted:
                self._warn_partial_budget("discover_keys")
                budget_warning_emitted = True

        def consume_rows(
            rows: list[dict[str, Any]],
            *,
            json_mode: JsonAttributeMode,
        ) -> bool:
            """Merge one independently verified lane; report a usable key."""

            nonlocal truncated
            usable_key_seen = False
            for row in rows:
                identity = self._physical_identity(row)
                if not self._row_is_active_in_window(row, overall_start, overall_end):
                    latest_keys.pop(identity, None)
                    continue
                if exact_key is not None:
                    decoded = self._decode_target_value(
                        row,
                        json_attribute_mode=json_mode,
                    )
                    if (
                        json_mode != "none"
                        and decoded is None
                        and self._target_value_is_unsupported(
                            row,
                            json_attribute_mode=json_mode,
                        )
                    ):
                        truncated = True
                    if decoded is None:
                        latest_keys.setdefault(identity, {})
                        continue
                    current = latest_keys.setdefault(identity, {})
                    prior_type = current.get(exact_key)
                    if (
                        prior_type is None
                        or _TYPE_PRIORITY[decoded[0]] < _TYPE_PRIORITY[prior_type]
                    ):
                        current[exact_key] = decoded[0]
                    usable_key_seen = True
                    continue

                row_keys, unsupported_value_seen = self._browse_row_keys(
                    row,
                    json_attribute_mode=json_mode,
                )
                current = latest_keys.setdefault(identity, {})
                for key, attr_type in row_keys.items():
                    prior_type = current.get(key)
                    if (
                        prior_type is None
                        or _TYPE_PRIORITY[attr_type] < _TYPE_PRIORITY[prior_type]
                    ):
                        current[key] = attr_type
                usable_key_seen = usable_key_seen or bool(row_keys)
                truncated = truncated or unsupported_value_seen
            return usable_key_seen

        # Every lane gets a cheap storage-order first probe in every adaptive
        # band. Exact-key probes that replay entirely stale/cleared state are
        # queued for deterministic continuation only after all bands have had
        # their fair first chance.
        fallback_states: list[dict[str, Any]] = []
        exact_found = False
        for segment in windows:
            for lane_name, predicate, replay_sql, json_mode, timeout_ms in lanes:
                if lane_name == "json" and not json_lane_available:
                    continue
                try:
                    candidate_ids, segment_truncated = self._candidate_ids(
                        projects,
                        segment,
                        predicate=predicate,
                        attribute_key=exact_key,
                        stratified=exact_key is None,
                        candidate_limit=ATTRIBUTE_READ_CANDIDATE_LIMIT,
                        query_timeout_ms=timeout_ms,
                    )
                    rows = self._verify_latest(
                        sql=replay_sql,
                        project_ids=projects,
                        candidate_ids=candidate_ids,
                        attribute_key=exact_key,
                        query_timeout_ms=timeout_ms,
                    )
                except Exception as exc:
                    if lane_name == "json" and is_read_budget_error(exc):
                        # JSON overflow has no skip index. Its independent short
                        # lane may degrade, but must never erase verified typed
                        # Map keys such as ``final_status``.
                        json_lane_available = False
                        mark_json_budget_exceeded()
                        continue
                    if latest_keys and is_read_budget_error(exc):
                        typed_lane_halted = True
                        mark_budget_exceeded()
                        break
                    raise

                covered_start = min(covered_start, segment[0])
                lane_found = consume_rows(rows, json_mode=json_mode)
                if exact_key is None:
                    truncated = truncated or segment_truncated
                    if lane_found and segment_truncated:
                        # Discovery pickers need a useful inventory, not an
                        # accounting scan. A verified dense page is sufficient
                        # and its sentinel makes the partial coverage explicit.
                        break
                elif lane_found:
                    exact_found = True
                    truncated = truncated or segment_truncated
                    break
                elif segment_truncated:
                    fallback_states.append(
                        {
                            "lane_name": lane_name,
                            "predicate": predicate,
                            "replay_sql": replay_sql,
                            "json_mode": json_mode,
                            "timeout_ms": timeout_ms,
                            "segment": segment,
                            "before_identity": None,
                            "pages": 0,
                            "complete": False,
                        }
                    )

            if (
                exact_found
                or typed_lane_halted
                or (exact_key is None and lane_found and segment_truncated)
            ):
                break
            discovered_key_count = len(
                {key for keys in latest_keys.values() for key in keys}
            )
            if exact_key is None and discovered_key_count > max_keys:
                truncated = True
                break

        # Restart stale-only exact probes from a deterministic ordered first
        # page, then keyset-page them. The unordered/storage-order cursor is
        # deliberately never reused as an ordered cursor.
        ordered_pages = 0
        while (
            exact_key is not None
            and not exact_found
            and not typed_lane_halted
            and ordered_pages < ATTRIBUTE_READ_TARGETED_CANDIDATE_PAGE_LIMIT
            and any(not state["complete"] for state in fallback_states)
        ):
            progressed = False
            for state in fallback_states:
                if state["complete"]:
                    continue
                if ordered_pages >= ATTRIBUTE_READ_TARGETED_CANDIDATE_PAGE_LIMIT:
                    break
                if state["lane_name"] == "json" and not json_lane_available:
                    state["complete"] = True
                    continue
                try:
                    candidate_ids, segment_truncated = self._candidate_ids(
                        projects,
                        state["segment"],
                        predicate=state["predicate"],
                        attribute_key=exact_key,
                        ordered=True,
                        before_identity=state["before_identity"],
                        candidate_limit=ATTRIBUTE_READ_CANDIDATE_LIMIT,
                        query_timeout_ms=state["timeout_ms"],
                    )
                    rows = self._verify_latest(
                        sql=state["replay_sql"],
                        project_ids=projects,
                        candidate_ids=candidate_ids,
                        attribute_key=exact_key,
                        query_timeout_ms=state["timeout_ms"],
                    )
                except Exception as exc:
                    if state["lane_name"] == "json" and is_read_budget_error(exc):
                        json_lane_available = False
                        state["complete"] = True
                        mark_json_budget_exceeded()
                        continue
                    if latest_keys and is_read_budget_error(exc):
                        typed_lane_halted = True
                        mark_budget_exceeded()
                        break
                    raise

                progressed = True
                ordered_pages += 1
                state["pages"] += 1
                covered_start = min(covered_start, state["segment"][0])
                lane_found = consume_rows(rows, json_mode=state["json_mode"])
                if lane_found:
                    exact_found = True
                    truncated = truncated or segment_truncated
                    state["complete"] = True
                    break
                if not segment_truncated:
                    state["complete"] = True
                elif (
                    not candidate_ids
                    or state["pages"] >= ATTRIBUTE_READ_TARGETED_CANDIDATE_PAGE_LIMIT
                ):
                    truncated = True
                    state["complete"] = True
                else:
                    state["before_identity"] = candidate_ids[-1]

            if exact_found or typed_lane_halted or not progressed:
                break

        if (
            exact_key is not None
            and not exact_found
            and any(not state["complete"] for state in fallback_states)
        ):
            truncated = True

        counts: Counter[tuple[str, AttributeType]] = Counter()
        key_totals: Counter[str] = Counter()
        for keys in latest_keys.values():
            for key, attr_type in keys.items():
                counts[(key, attr_type)] += 1
                key_totals[key] += 1

        type_counts: dict[str, list[tuple[AttributeType, int]]] = defaultdict(list)
        for (key, attr_type), count in counts.items():
            type_counts[key].append((attr_type, count))
        rows = [
            AttributeKeyRow(
                key=key,
                type=min(
                    candidates,
                    key=lambda item: (-item[1], _TYPE_PRIORITY[item[0]]),
                )[0],
                count=key_totals[key],
            )
            for key, candidates in type_counts.items()
        ]
        if order_by_count_desc:
            rows.sort(key=lambda row: (-row.count, row.key.casefold(), row.key))
        else:
            rows.sort(key=lambda row: (row.key.casefold(), row.key, row.type))
        if len(rows) > max_keys:
            rows = rows[:max_keys]
            truncated = True
        # A short JSON-overflow lane timing out after verified typed Map data is
        # a usable sampled response, not a failed picker. Keep the incomplete
        # coverage explicit without inviting clients to discard ``final_status``
        # and other typed results. With no usable result, retain the stronger
        # budget signal.
        usable_json_degradation = json_budget_exceeded and bool(rows)
        effective_budget_exceeded = budget_exceeded or (
            json_budget_exceeded and not rows
        )
        effective_truncated = truncated or usable_json_degradation
        return AttributeKeyRead(
            tuple(rows),
            self._metadata(
                complete=not effective_truncated and not effective_budget_exceeded,
                error_code=(
                    "read_budget_exceeded"
                    if effective_budget_exceeded
                    else "sample_limit"
                    if effective_truncated
                    else None
                ),
                sampled=(
                    effective_truncated and not effective_budget_exceeded and bool(rows)
                ),
                window_start=covered_start,
                window_end=overall_end,
                query_count=self._query_count,
            ),
        )

    def read_values(
        self,
        project_ids: Iterable[Any],
        key: str,
        *,
        search: str | None = None,
        max_values: int = ATTRIBUTE_READ_MAX_VALUES,
        horizon_days: int = 365,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
    ) -> AttributeValueRead:
        self._begin_operation()
        projects = self._project_ids(project_ids)
        key = validate_attribute_key(key)
        normalized_search = validate_attribute_search(search or "")
        max_values = min(max(int(max_values), 1), ATTRIBUTE_READ_MAX_VALUES)
        windows = self._windows(
            horizon_days=horizon_days,
            window_start=window_start,
            window_end=window_end,
        )
        overall_start, overall_end = windows[-1][0], windows[0][1]
        if not projects:
            return AttributeValueRead(
                (),
                self._metadata(
                    complete=True,
                    error_code=None,
                    window_start=overall_start,
                    window_end=overall_end,
                    query_count=self._query_count,
                ),
            )

        typed_predicate = (
            "(indexHint(has(mapKeys(attrs_string), %(attribute_key)s)) "
            "AND has(attrs_string.keys, %(attribute_key)s)) "
            "OR (indexHint(has(mapKeys(attrs_number), %(attribute_key)s)) "
            "AND has(attrs_number.keys, %(attribute_key)s)) "
            "OR (indexHint(has(mapKeys(attrs_bool), %(attribute_key)s)) "
            "AND has(attrs_bool.keys, %(attribute_key)s))"
        )
        json_predicate = "JSONHas(attributes_extra, %(attribute_key)s)"
        pushed_search: str | None = None
        # Typed Map acquisition remains key-only; exact search is applied in
        # Python after finite latest-state replay. Preserve the existing ASCII
        # pushdown only for the independent JSON lane.
        if normalized_search and normalized_search.isascii():
            pushed_search = normalized_search
            json_predicate = (
                "JSONHas(attributes_extra, %(attribute_key)s) "
                "AND positionCaseInsensitiveUTF8("
                "JSONExtractRaw(attributes_extra, %(attribute_key)s), "
                "%(attribute_search)s) > 0"
            )
        lanes: list[tuple[str, str, str, JsonAttributeMode, int | None]] = [
            (
                "typed",
                typed_predicate,
                _LATEST_TYPED_TARGET_SQL,
                "none",
                None,
            )
        ]
        if self._reads_json_overflow:
            lanes.append(
                (
                    "json",
                    json_predicate,
                    _LATEST_TARGET_SQL,
                    self._json_attribute_mode,
                    ATTRIBUTE_READ_JSON_QUERY_TIMEOUT_MS,
                )
            )

        latest_values: dict[PhysicalSpanIdentity, tuple[AttributeType, Any]] = {}
        truncated = False
        budget_exceeded = False
        json_budget_exceeded = False
        budget_warning_emitted = False
        covered_start = overall_end
        json_lane_available = self._reads_json_overflow
        typed_lane_halted = False

        def mark_budget_exceeded() -> None:
            nonlocal budget_exceeded, budget_warning_emitted
            budget_exceeded = True
            if not budget_warning_emitted:
                self._warn_partial_budget("read_values")
                budget_warning_emitted = True

        def mark_json_budget_exceeded() -> None:
            nonlocal json_budget_exceeded, budget_warning_emitted
            json_budget_exceeded = True
            if not budget_warning_emitted:
                self._warn_partial_budget("read_values")
                budget_warning_emitted = True

        needle = normalized_search.casefold()

        def decoded_has_usable_value(decoded: tuple[AttributeType, Any]) -> bool:
            attr_type, value = decoded
            candidates: tuple[Any, ...]
            if attr_type == "array":
                if not isinstance(value, tuple):
                    return False
                candidates = value
            else:
                if value in (None, ""):
                    return False
                candidates = (value,)
            return any(
                not needle or needle in _value_search_text(candidate).casefold()
                for candidate in candidates
            )

        def consume_rows(
            rows: list[dict[str, Any]],
            *,
            json_mode: JsonAttributeMode,
        ) -> bool:
            """Merge a verified lane and report whether it yielded a usable value."""

            nonlocal truncated
            usable_value_seen = False
            for row in rows:
                identity = self._physical_identity(row)
                if not self._row_is_active_in_window(row, overall_start, overall_end):
                    latest_values.pop(identity, None)
                    continue
                decoded = self._decode_target_value(
                    row,
                    json_attribute_mode=json_mode,
                )
                if (
                    json_mode != "none"
                    and decoded is None
                    and self._target_value_is_unsupported(
                        row,
                        json_attribute_mode=json_mode,
                    )
                ):
                    truncated = True
                if decoded is None or (
                    decoded[0] != "array" and decoded[1] in (None, "")
                ):
                    continue
                prior = latest_values.get(identity)
                if (
                    prior is None
                    or _TYPE_PRIORITY[decoded[0]] < _TYPE_PRIORITY[prior[0]]
                ):
                    latest_values[identity] = decoded
                usable_value_seen = usable_value_seen or decoded_has_usable_value(
                    decoded
                )
            return usable_value_seen

        # Phase one gives each lane and adaptive band one storage-order probe.
        # This is the normal fast path and guarantees older bands are not
        # starved by a dense recent week. Only truncated pages whose latest-state
        # replay produced no usable value need deterministic continuation.
        fallback_states: list[dict[str, Any]] = []
        candidate_pages = 0
        usable_sample_found = False
        for segment in windows:
            for lane_name, predicate, replay_sql, json_mode, timeout_ms in lanes:
                if candidate_pages >= ATTRIBUTE_READ_VALUE_TOTAL_CANDIDATE_PAGE_LIMIT:
                    truncated = True
                    break
                if lane_name == "json" and not json_lane_available:
                    continue
                try:
                    candidate_ids, segment_truncated = self._candidate_ids(
                        projects,
                        segment,
                        predicate=predicate,
                        attribute_key=key,
                        attribute_search=(
                            pushed_search if lane_name == "json" else None
                        ),
                        candidate_limit=ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT,
                        query_timeout_ms=timeout_ms,
                    )
                    rows = self._verify_latest(
                        sql=replay_sql,
                        project_ids=projects,
                        candidate_ids=candidate_ids,
                        attribute_key=key,
                        query_timeout_ms=timeout_ms,
                    )
                except Exception as exc:
                    if lane_name == "json" and is_read_budget_error(exc):
                        json_lane_available = False
                        mark_json_budget_exceeded()
                        continue
                    if latest_values and is_read_budget_error(exc):
                        typed_lane_halted = True
                        mark_budget_exceeded()
                        break
                    raise

                candidate_pages += 1
                covered_start = min(covered_start, segment[0])
                usable_value_seen = consume_rows(rows, json_mode=json_mode)
                if segment_truncated and usable_value_seen:
                    # The picker has useful verified values. Stop immediately
                    # instead of scanning JSON and older bands; the sentinel
                    # keeps the intentionally partial distribution honest.
                    truncated = True
                    usable_sample_found = True
                    break
                elif segment_truncated:
                    fallback_states.append(
                        {
                            "lane_name": lane_name,
                            "predicate": predicate,
                            "replay_sql": replay_sql,
                            "json_mode": json_mode,
                            "timeout_ms": timeout_ms,
                            "segment": segment,
                            "before_identity": None,
                            "pages": 0,
                            "complete": False,
                        }
                    )

            if typed_lane_halted or usable_sample_found:
                break
        if candidate_pages >= ATTRIBUTE_READ_VALUE_TOTAL_CANDIDATE_PAGE_LIMIT and any(
            not state["complete"] for state in fallback_states
        ):
            truncated = True

        # Phase two round-robins only the stale-only truncated lanes. It restarts
        # each lane at ordered page one; a cursor is derived exclusively from a
        # preceding page with that same deterministic order.
        while (
            not typed_lane_halted
            and candidate_pages < ATTRIBUTE_READ_VALUE_TOTAL_CANDIDATE_PAGE_LIMIT
            and any(not state["complete"] for state in fallback_states)
        ):
            progressed = False
            for state in fallback_states:
                if state["complete"]:
                    continue
                if candidate_pages >= ATTRIBUTE_READ_VALUE_TOTAL_CANDIDATE_PAGE_LIMIT:
                    break
                if state["lane_name"] == "json" and not json_lane_available:
                    state["complete"] = True
                    continue
                if state["pages"] >= ATTRIBUTE_READ_VALUE_CANDIDATE_PAGE_LIMIT:
                    truncated = True
                    state["complete"] = True
                    continue
                try:
                    candidate_ids, segment_truncated = self._candidate_ids(
                        projects,
                        state["segment"],
                        predicate=state["predicate"],
                        attribute_key=key,
                        attribute_search=(
                            pushed_search if state["lane_name"] == "json" else None
                        ),
                        ordered=True,
                        before_identity=state["before_identity"],
                        candidate_limit=ATTRIBUTE_READ_VALUE_CANDIDATE_LIMIT,
                        query_timeout_ms=state["timeout_ms"],
                    )
                    rows = self._verify_latest(
                        sql=state["replay_sql"],
                        project_ids=projects,
                        candidate_ids=candidate_ids,
                        attribute_key=key,
                        query_timeout_ms=state["timeout_ms"],
                    )
                except Exception as exc:
                    if state["lane_name"] == "json" and is_read_budget_error(exc):
                        json_lane_available = False
                        state["complete"] = True
                        mark_json_budget_exceeded()
                        continue
                    if latest_values and is_read_budget_error(exc):
                        typed_lane_halted = True
                        mark_budget_exceeded()
                        break
                    raise

                progressed = True
                candidate_pages += 1
                state["pages"] += 1
                covered_start = min(covered_start, state["segment"][0])
                usable_value_seen = consume_rows(rows, json_mode=state["json_mode"])
                if usable_value_seen:
                    truncated = truncated or segment_truncated
                    state["complete"] = True
                elif not segment_truncated:
                    state["complete"] = True
                elif not candidate_ids:
                    truncated = True
                    state["complete"] = True
                elif state["pages"] >= ATTRIBUTE_READ_VALUE_CANDIDATE_PAGE_LIMIT:
                    truncated = True
                    state["complete"] = True
                else:
                    state["before_identity"] = candidate_ids[-1]

            if typed_lane_halted or not progressed:
                break

        if any(not state["complete"] for state in fallback_states):
            truncated = True

        counts: Counter[tuple[AttributeType, str]] = Counter()
        values: dict[tuple[AttributeType, str], AttributeValue] = {}
        for attr_type, value in latest_values.values():
            candidates: tuple[AttributeValue, ...]
            if attr_type == "array":
                if not isinstance(value, tuple):
                    truncated = True
                    continue
                if len(value) > max_values:
                    truncated = True
                candidates = value
            else:
                candidates = (value,)
            # Count an array member at most once per physical span.  Repeated
            # members do not represent additional spans in the value picker.
            seen_in_span: set[tuple[AttributeType, str]] = set()
            for candidate in candidates:
                display = _value_search_text(candidate)
                if needle and needle not in display.casefold():
                    continue
                canonical = _canonical_value(attr_type, candidate)
                identity = (attr_type, canonical)
                if identity in seen_in_span:
                    continue
                seen_in_span.add(identity)
                counts[identity] += 1
                values[identity] = candidate

        ordered = sorted(
            counts,
            key=lambda item: (
                -counts[item],
                _value_search_text(values[item]).casefold(),
                _value_search_text(values[item]),
                _TYPE_PRIORITY[item[0]],
            ),
        )
        if len(ordered) > max_values:
            ordered = ordered[:max_values]
            truncated = True
        usable_json_degradation = json_budget_exceeded and bool(ordered)
        effective_budget_exceeded = budget_exceeded or (
            json_budget_exceeded and not ordered
        )
        effective_truncated = truncated or usable_json_degradation
        return AttributeValueRead(
            tuple(
                AttributeValueRow(
                    value=values[identity],
                    type=identity[0],
                    count=counts[identity],
                )
                for identity in ordered
            ),
            self._metadata(
                complete=not effective_truncated and not effective_budget_exceeded,
                error_code=(
                    "read_budget_exceeded"
                    if effective_budget_exceeded
                    else "sample_limit"
                    if effective_truncated
                    else None
                ),
                sampled=(
                    effective_truncated
                    and not effective_budget_exceeded
                    and bool(ordered)
                ),
                window_start=covered_start,
                window_end=overall_end,
                query_count=self._query_count,
            ),
        )

    def read_detail(
        self,
        project_ids: Iterable[Any],
        key: str,
        *,
        horizon_days: int = 365,
    ) -> AttributeDetailRead:
        """Read a bounded, latest-state distribution for one typed attribute.

        A key can exist in more than one typed Map/JSON family across spans.
        Preserve the detail endpoint's historical dominant-type contract while
        deriving it only from active latest rows. Stable typed-Map-before-array
        priority resolves equal occurrence counts.
        """

        value_read = self.read_values(
            project_ids,
            key,
            max_values=ATTRIBUTE_READ_MAX_VALUES,
            horizon_days=horizon_days,
        )
        type_totals: Counter[AttributeType] = Counter()
        for row in value_read.rows:
            type_totals[row.type] += row.count
        if not type_totals:
            return AttributeDetailRead(None, (), value_read.metadata)
        attribute_type = min(
            type_totals,
            key=lambda item: (-type_totals[item], _TYPE_PRIORITY[item]),
        )
        return AttributeDetailRead(
            attribute_type,
            tuple(row for row in value_read.rows if row.type == attribute_type),
            value_read.metadata,
        )

    def sample_cardinality(
        self,
        project_ids: Iterable[Any],
        *,
        horizon_days: int = 30,
    ) -> AttributeCardinalityRead:
        """Sample nested picker dimensions from CH only under one operation budget."""

        self._begin_operation()
        projects = self._project_ids(project_ids)
        windows = self._windows(
            horizon_days=horizon_days,
            window_start=None,
            window_end=None,
        )
        overall_start, overall_end = windows[-1][0], windows[0][1]
        if not projects:
            return AttributeCardinalityRead(
                0,
                0,
                self._metadata(
                    complete=True,
                    error_code=None,
                    window_start=overall_start,
                    window_end=overall_end,
                    query_count=self._query_count,
                ),
            )

        latest_rows: dict[PhysicalSpanIdentity, dict[str, Any]] = {}
        truncated = False
        budget_exceeded = False
        covered_start = overall_end
        for segment in windows:
            try:
                candidate_ids, segment_truncated = self._candidate_ids(
                    projects,
                    segment,
                    predicate="1",
                    attribute_key=None,
                    stratified=True,
                    candidate_limit=ATTRIBUTE_READ_CANDIDATE_LIMIT,
                )
                rows = self._verify_latest(
                    sql=_LATEST_CARDINALITY_SQL,
                    project_ids=projects,
                    candidate_ids=candidate_ids,
                )
            except Exception as exc:
                if not latest_rows or not is_read_budget_error(exc):
                    raise
                budget_exceeded = True
                self._warn_partial_budget("sample_cardinality")
                break
            covered_start = segment[0]
            truncated = truncated or segment_truncated
            for row in rows:
                identity = self._physical_identity(row)
                if self._row_is_active_in_window(row, overall_start, overall_end):
                    latest_rows[identity] = row
                else:
                    latest_rows.pop(identity, None)

            if segment_truncated:
                break

        spans_by_trace: Counter[tuple[str, str]] = Counter()
        traces_by_session: dict[tuple[str, str], set[str]] = defaultdict(set)
        for row in latest_rows.values():
            project_id = str(row.get("project_id") or "")
            trace_id = str(row.get("trace_id") or "")
            session_id = str(row.get("trace_session_id") or "")
            if not trace_id:
                continue
            spans_by_trace[(project_id, trace_id)] += 1
            if session_id and session_id != "00000000-0000-0000-0000-000000000000":
                traces_by_session[(project_id, session_id)].add(trace_id)
        return AttributeCardinalityRead(
            max(spans_by_trace.values(), default=0),
            max(
                (len(trace_ids) for trace_ids in traces_by_session.values()), default=0
            ),
            self._metadata(
                complete=not truncated and not budget_exceeded,
                error_code=(
                    "read_budget_exceeded"
                    if budget_exceeded
                    else "sample_limit"
                    if truncated
                    else None
                ),
                sampled=truncated and not budget_exceeded,
                window_start=covered_start,
                window_end=overall_end,
                query_count=self._query_count,
            ),
        )


def _canonical_value(attr_type: AttributeType, value: Any) -> str:
    if attr_type == "boolean":
        return "true" if bool(value) else "false"
    if attr_type == "number":
        return json.dumps(float(value), allow_nan=False, separators=(",", ":"))
    if attr_type == "array":
        # Array picker rows are individual JSON scalar members.  Preserve
        # their JSON type so ``1``, ``1.0``, ``true`` and ``"1"`` never merge.
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )
    return json.dumps(str(value), ensure_ascii=False, separators=(",", ":"))


def _value_search_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def merge_read_metadata(
    *metadata: AttributeReadMetadata,
) -> AttributeReadMetadata:
    """Merge multiple selector phases without hiding a degraded phase."""

    if not metadata:
        raise ValueError("At least one metadata value is required")
    complete = all(item.query_complete for item in metadata)
    degraded_metadata = next(
        (item for item in metadata if item.query_status == "degraded"),
        None,
    )
    has_sampled_metadata = any(item.query_status == "sampled" for item in metadata)
    error_code = (
        degraded_metadata.query_error_code
        if degraded_metadata is not None
        else next(
            (item.query_error_code for item in metadata if item.query_error_code),
            None,
        )
    )
    query_status: QueryStatus = "complete"
    if not complete:
        query_status = (
            "sampled"
            if degraded_metadata is None and has_sampled_metadata
            else "degraded"
        )
    return AttributeReadMetadata(
        query_complete=complete,
        query_status=query_status,
        query_error_code=error_code,
        query_window_start=min(item.query_window_start for item in metadata),
        query_window_end=max(item.query_window_end for item in metadata),
        query_count=sum(item.query_count for item in metadata),
    )


__all__ = [
    "ATTRIBUTE_READ_HORIZON_DAYS",
    "ATTRIBUTE_READ_MAX_PROJECTS",
    "ATTRIBUTE_READ_SETTINGS",
    "AttributeCardinalityRead",
    "AttributeKeyInventory",
    "AttributeKeyRead",
    "AttributeKeyRow",
    "AttributeQueryPage",
    "AttributeReadMetadata",
    "AttributeReadSelector",
    "AttributeValueRead",
    "AttributeValueRow",
    "IncompleteLatestStateReplay",
    "InvalidAttributeKey",
    "InvalidAttributeSearch",
    "V2AttributeQueryExecutor",
    "adaptive_attribute_windows",
    "merge_read_metadata",
    "validate_attribute_key",
    "validate_attribute_search",
]
