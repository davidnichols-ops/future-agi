"""One-statement exact latest-state span attribute detail aggregation.

The spans table is direct-write ReplacingMergeTree data.  Mutable predicates
(deletion and key presence) must therefore run *after* argMax has selected the
latest version of every immutable physical span identity.  In particular, a
later key removal or tombstone must win over an older key-bearing row.

This reader is intended for the existing exact-aggregation background worker;
HTTP requests serve/poll the last atomically published snapshot and never wait
for a full tenant scan.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any

from tracer.services.clickhouse.attribute_reads import (
    V2AttributeQueryExecutor,
    validate_attribute_key,
)

EXACT_ATTRIBUTE_DETAIL_HORIZON_DAYS = 365
EXACT_ATTRIBUTE_DETAIL_TOP_VALUES = 100
EXACT_ATTRIBUTE_DETAIL_QUERY_TIMEOUT_MS = 3_300_000
_MIB = 1024 * 1024
EXACT_ATTRIBUTE_DETAIL_READ_SETTINGS: dict[str, Any] = {
    "max_threads": 1,
    "max_block_size": 512,
    "preferred_block_size_bytes": 4 * _MIB,
    "preferred_max_column_in_block_size_bytes": 4 * _MIB,
    "optimize_aggregation_in_order": 1,
    "max_bytes_before_external_group_by": 32 * _MIB,
    "max_bytes_before_external_sort": 32 * _MIB,
    "optimize_use_projections": 0,
    "allow_experimental_projection_optimization": 0,
    "max_rows_to_read": 0,
    "max_bytes_to_read": 0,
    "max_memory_usage": 1536 * _MIB,
    "read_overflow_mode": "throw",
    "max_result_rows": 1_001,
    "max_result_bytes": 64 * _MIB,
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
}

_TYPE_PRIORITY: dict[str, int] = {
    "string": 0,
    "number": 1,
    "boolean": 2,
    "array": 3,
    "map": 4,
    "json": 5,
}


EXACT_ATTRIBUTE_DETAIL_SQL = r"""
WITH
latest_spans AS
(
    SELECT
        project_id,
        trace_id,
        id,
        start_time,
        argMax(
            tuple(
                is_deleted,
                attrs_string,
                attrs_number,
                attrs_bool,
                attributes_extra
            ),
            _version
        ) AS latest_state
    FROM spans AS attribute_source
    PREWHERE attribute_source.project_id = toUUID(%(project_id)s)
      AND attribute_source.start_time >= %(window_start)s
      AND attribute_source.start_time < %(window_end)s
    GROUP BY project_id, trace_id, id, start_time
),
active_values AS
(
    SELECT
        'string' AS attribute_type,
        toJSONString(tupleElement(latest_state, 2)[%(attribute_key)s]) AS value_json,
        CAST(NULL, 'Nullable(Float64)') AS number_value
    FROM latest_spans
    WHERE tupleElement(latest_state, 1) = 0
      AND mapContains(tupleElement(latest_state, 2), %(attribute_key)s)

    UNION ALL

    SELECT
        'number' AS attribute_type,
        toJSONString(tupleElement(latest_state, 3)[%(attribute_key)s]) AS value_json,
        toFloat64(tupleElement(latest_state, 3)[%(attribute_key)s]) AS number_value
    FROM latest_spans
    WHERE tupleElement(latest_state, 1) = 0
      AND mapContains(tupleElement(latest_state, 3), %(attribute_key)s)

    UNION ALL

    SELECT
        'boolean' AS attribute_type,
        if(tupleElement(latest_state, 4)[%(attribute_key)s], 'true', 'false') AS value_json,
        CAST(NULL, 'Nullable(Float64)') AS number_value
    FROM latest_spans
    WHERE tupleElement(latest_state, 1) = 0
      AND mapContains(tupleElement(latest_state, 4), %(attribute_key)s)

    UNION ALL

    SELECT
        multiIf(
            startsWith(trimLeft(raw_value), '['), 'array',
            startsWith(trimLeft(raw_value), '{'), 'map',
            'json'
        ) AS attribute_type,
        raw_value AS value_json,
        CAST(NULL, 'Nullable(Float64)') AS number_value
    FROM
    (
        SELECT
            JSONExtractRaw(
                tupleElement(latest_state, 5),
                %(attribute_key)s
            ) AS raw_value
        FROM latest_spans
        WHERE tupleElement(latest_state, 1) = 0
          AND JSONHas(tupleElement(latest_state, 5), %(attribute_key)s)
    )
    WHERE raw_value != ''
),
grouped_values AS
(
    SELECT
        attribute_type,
        value_json,
        any(number_value) AS number_value,
        count() AS value_count
    FROM active_values
    GROUP BY attribute_type, value_json
),
type_statistics AS
(
    SELECT
        attribute_type,
        sum(value_count) AS type_count,
        count() AS unique_values,
        minIf(number_value, isNotNull(number_value)) AS numeric_min,
        maxIf(number_value, isNotNull(number_value)) AS numeric_max,
        if(
            sumIf(value_count, isNotNull(number_value)) = 0,
            CAST(NULL, 'Nullable(Float64)'),
            sumIf(number_value * value_count, isNotNull(number_value))
                / sumIf(value_count, isNotNull(number_value))
        ) AS numeric_avg,
        quantileExactWeightedIf(0.50)(
            number_value, value_count, isNotNull(number_value)
        ) AS numeric_p50,
        quantileExactWeightedIf(0.95)(
            number_value, value_count, isNotNull(number_value)
        ) AS numeric_p95
    FROM grouped_values
    GROUP BY attribute_type
),
ranked_values AS
(
    SELECT
        attribute_type,
        value_json,
        value_count,
        row_number() OVER (
            PARTITION BY attribute_type
            ORDER BY value_count DESC, value_json ASC
        ) AS value_rank
    FROM grouped_values
)
SELECT
    ranked_values.attribute_type AS attribute_type,
    ranked_values.value_json AS value_json,
    ranked_values.value_count AS value_count,
    type_statistics.type_count AS type_count,
    type_statistics.unique_values AS unique_values,
    type_statistics.numeric_min AS numeric_min,
    type_statistics.numeric_max AS numeric_max,
    type_statistics.numeric_avg AS numeric_avg,
    type_statistics.numeric_p50 AS numeric_p50,
    type_statistics.numeric_p95 AS numeric_p95
FROM ranked_values
INNER JOIN type_statistics USING (attribute_type)
WHERE ranked_values.value_rank <= %(top_values_limit)s
ORDER BY
    type_statistics.type_count DESC,
    indexOf(['string', 'number', 'boolean', 'array', 'map', 'json'], attribute_type),
    ranked_values.value_rank ASC
"""


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decode_json_value(raw: Any) -> Any:
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        # ClickHouse toJSONString/JSONExtractRaw should always be valid JSON;
        # retaining text is safer than discarding a completed aggregate if an
        # old malformed overflow payload is encountered.
        return raw


def read_exact_attribute_detail(
    *,
    project_id: str,
    attribute_key: str,
    executor: V2AttributeQueryExecutor | None = None,
    window_end: datetime | None = None,
    horizon_days: int = EXACT_ATTRIBUTE_DETAIL_HORIZON_DAYS,
) -> dict[str, Any]:
    """Compute one complete exact detail payload in one ClickHouse statement."""

    started = monotonic()
    key = validate_attribute_key(attribute_key)
    end = _utc(window_end or datetime.now(UTC))
    start = end - timedelta(days=max(1, min(int(horizon_days), 365)))
    query_executor = executor or V2AttributeQueryExecutor()
    page = query_executor.execute(
        EXACT_ATTRIBUTE_DETAIL_SQL,
        {
            "project_id": str(project_id),
            "attribute_key": key,
            "window_start": start,
            "window_end": end,
            "top_values_limit": EXACT_ATTRIBUTE_DETAIL_TOP_VALUES,
        },
        timeout_ms=EXACT_ATTRIBUTE_DETAIL_QUERY_TIMEOUT_MS,
        settings=EXACT_ATTRIBUTE_DETAIL_READ_SETTINGS,
    )
    rows = list(page.data or [])
    by_type: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        attribute_type = str(row.get("attribute_type") or "")
        if attribute_type not in _TYPE_PRIORITY:
            raise RuntimeError("exact attribute detail returned an invalid type")
        by_type.setdefault(attribute_type, []).append(row)

    common = {
        "key": key,
        "query_complete": True,
        "query_status": "complete",
        "query_sampled": False,
        "query_window_start": start.isoformat().replace("+00:00", "Z"),
        "query_window_end": end.isoformat().replace("+00:00", "Z"),
        "query_count": 1,
        "query_elapsed_ms": round((monotonic() - started) * 1000, 3),
    }
    if not by_type:
        return {
            **common,
            "type": None,
            "count": 0,
            "unique_values": 0,
            "top_values": [],
        }

    attribute_type = min(
        by_type,
        key=lambda value: (
            -int(by_type[value][0].get("type_count") or 0),
            _TYPE_PRIORITY[value],
        ),
    )
    selected = by_type[attribute_type]
    total = int(selected[0].get("type_count") or 0)
    top_values = [
        {
            "value": _decode_json_value(row.get("value_json")),
            "count": int(row.get("value_count") or 0),
            "percentage": (
                float(row.get("value_count") or 0) * 100.0 / total
                if total
                else 0.0
            ),
        }
        for row in selected
    ]
    payload: dict[str, Any] = {
        **common,
        "type": attribute_type,
        "count": total,
        "unique_values": int(selected[0].get("unique_values") or 0),
        "top_values": top_values,
    }
    if attribute_type == "number":
        stats = {
            "min": selected[0].get("numeric_min"),
            "max": selected[0].get("numeric_max"),
            "avg": selected[0].get("numeric_avg"),
            "p50": selected[0].get("numeric_p50"),
            "p95": selected[0].get("numeric_p95"),
        }
        payload.update(stats)
        payload["stats"] = stats
    return payload


__all__ = [
    "EXACT_ATTRIBUTE_DETAIL_HORIZON_DAYS",
    "EXACT_ATTRIBUTE_DETAIL_QUERY_TIMEOUT_MS",
    "EXACT_ATTRIBUTE_DETAIL_READ_SETTINGS",
    "EXACT_ATTRIBUTE_DETAIL_SQL",
    "read_exact_attribute_detail",
]
