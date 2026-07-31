from datetime import UTC, datetime
from typing import Any

from model_hub.models.choices import AnnotationTypeChoices
from model_hub.models.develop_annotations import AnnotationsLabels
from tracer.services.clickhouse.query_builders import (
    AnnotationGraphQueryBuilder,
    TimeSeriesQueryBuilder,
)
from tracer.services.clickhouse.read_budget import is_read_budget_error
from tracer.utils.helper import get_annotation_labels_for_project

GRAPH_READ_TIMEOUT_MS = 750
GRAPH_READ_SETTINGS = {
    "max_threads": 2,
    "max_memory_usage": 256 * 1024 * 1024,
    "max_bytes_to_read": 1024 * 1024 * 1024,
    "read_overflow_mode": "throw",
    "max_result_rows": 2000,
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
}


def degraded_graph_response(metric_id: str, exc: Exception) -> dict[str, Any]:
    """Return a safe, machine-readable graph failure without leaking CH text."""
    error_code = "read_budget_exceeded" if is_read_budget_error(exc) else "query_failed"
    return {
        "metric_name": metric_id,
        "data": [],
        "query_complete": False,
        "query_status": "degraded",
        "query_error_code": error_code,
    }


def _utc_iso(value: datetime) -> str:
    """Serialize ClickHouse's naive-UTC boundaries without timezone ambiguity."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    return value.isoformat().replace("+00:00", "Z")


def normalize_eval_graph_output_type(req_data_config: dict[str, Any]) -> str:
    """Translate both backend enums and the frontend graph wire values."""
    raw_value = req_data_config.get("eval_output_type")
    if raw_value in (None, ""):
        raw_value = req_data_config.get("output_type", "SCORE")

    normalized = str(raw_value).strip().lower().replace("-", "_").replace("/", "_")
    normalized = "_".join(normalized.split())
    if normalized in {"bool", "pass_fail", "passfail"}:
        return "PASS_FAIL"
    if normalized in {"str_list", "choice", "choices"}:
        return "CHOICES"
    if normalized in {"float", "score", "percentage", "numeric", "number"}:
        return "SCORE"
    return "SCORE"


def _selected_bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() not in {
            "false",
            "failed",
            "fail",
            "no",
            "0",
        }
    return True


def _invert_pass_fail_rows(rows: list[Any], columns: list[str]) -> list[Any]:
    """Turn pass-rate rows into fail-rate rows before zero filling."""
    if not rows:
        return rows

    value_index = columns.index("value") if "value" in columns else 1
    inverted = []
    for row in rows:
        if isinstance(row, dict):
            updated = dict(row)
            value = updated.get("value")
            updated["value"] = None if value is None else 100.0 - float(value)
            inverted.append(updated)
            continue

        updated = list(row)
        if len(updated) > value_index and updated[value_index] is not None:
            updated[value_index] = 100.0 - float(updated[value_index])
        inverted.append(tuple(updated))
    return inverted


def _format_eval_graph_response(
    formatted: Any,
    *,
    metric_id: str,
    selected_choice: Any = None,
) -> dict[str, Any]:
    """Keep every observe graph endpoint on its contracted single-series shape."""
    series = formatted
    if isinstance(formatted, list):
        series = None
        if selected_choice not in (None, ""):
            choice_suffix = f" - {selected_choice}"
            series = next(
                (
                    item
                    for item in formatted
                    if str(item.get("name", "")).endswith(choice_suffix)
                ),
                None,
            )
        elif formatted:
            series = formatted[0]

    return {
        "metric_name": metric_id,
        "data": series.get("data", []) if isinstance(series, dict) else [],
    }


def format_system_metric_graph(
    ch_data: dict[str, list[dict[str, Any]]], metric_id: str
) -> dict[str, Any]:
    metric_key = metric_id if metric_id in ch_data else "latency"
    metric_points = ch_data.get(metric_key, [])
    traffic_points = ch_data.get("traffic", [])
    traffic_by_ts = {t.get("timestamp"): t.get("traffic", 0) for t in traffic_points}
    return {
        "metric_name": metric_id,
        "data": [
            {
                "timestamp": p.get("timestamp"),
                "value": p.get("value", 0),
                "primary_traffic": traffic_by_ts.get(p.get("timestamp"), 0),
            }
            for p in metric_points
        ],
    }


def annotation_output_type(label: AnnotationsLabels, requested: str = None) -> str:
    if requested:
        return requested
    if label.type in (
        AnnotationTypeChoices.NUMERIC.value,
        AnnotationTypeChoices.STAR.value,
    ):
        return "float"
    if label.type == AnnotationTypeChoices.THUMBS_UP_DOWN.value:
        return "bool"
    if label.type == AnnotationTypeChoices.CATEGORICAL.value:
        return "str_list"
    return "text"


def fetch_system_metric_graph_ch(
    *,
    analytics,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    metric_id: str,
    observe_type: str = "trace",
    timeout_ms: int = GRAPH_READ_TIMEOUT_MS,
) -> dict[str, Any]:
    builder = TimeSeriesQueryBuilder(
        project_id=str(project_id),
        filters=filters,
        interval=interval,
        observe_type=observe_type,
        metric_id=metric_id,
    )
    query, params = builder.build()
    result = analytics.execute_ch_query(
        query,
        params,
        timeout_ms=timeout_ms,
        settings=GRAPH_READ_SETTINGS,
    )
    ch_data = builder.format_result(result.data, result.columns or [])
    response = format_system_metric_graph(ch_data, metric_id)
    if builder.rollup_window_adjusted:
        assert builder.rollup_window_start is not None
        assert builder.rollup_window_end is not None
        response.update(
            {
                "query_complete": True,
                "query_status": "adjusted",
                "query_window_adjusted": True,
                "query_window_start": _utc_iso(builder.rollup_window_start),
                "query_window_end": _utc_iso(builder.rollup_window_end),
            }
        )
    return response


def fetch_eval_graph_ch(
    *,
    analytics,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    req_data_config: dict[str, Any],
    observe_type: str = "trace",
    timeout_ms: int = GRAPH_READ_TIMEOUT_MS,
) -> dict[str, Any]:
    from tracer.services.clickhouse.v2.query_builders.eval_metrics import (
        EvalMetricsQueryBuilderV2,
    )

    # These graph endpoints are CH25-only (their PostgreSQL fallback was
    # removed during the spans cutover), so do not let an absent rollout flag
    # silently select the legacy span schema.
    metric_id = str(req_data_config.get("id") or "")
    eval_output_type = normalize_eval_graph_output_type(req_data_config)
    selected_value = req_data_config.get("value")
    choices = list(req_data_config.get("choices") or [])
    if eval_output_type == "CHOICES":
        if selected_value not in (None, "") and not choices:
            choices = [str(selected_value)]
        if not choices:
            return {"metric_name": metric_id, "data": []}

    builder = EvalMetricsQueryBuilderV2(
        project_id=str(project_id),
        custom_eval_config_id=metric_id,
        filters=filters,
        interval=interval,
        eval_output_type=eval_output_type,
        choices=choices,
        observe_type=observe_type,
    )
    query, params = builder.build()
    result = analytics.execute_ch_query(
        query,
        params,
        timeout_ms=timeout_ms,
        settings=GRAPH_READ_SETTINGS,
    )
    columns = result.columns or []
    rows = result.data
    if eval_output_type == "PASS_FAIL" and not _selected_bool_value(selected_value):
        rows = _invert_pass_fail_rows(rows, columns)
    formatted = builder.format_result(rows, columns)
    return _format_eval_graph_response(
        formatted,
        metric_id=metric_id,
        selected_choice=selected_value if eval_output_type == "CHOICES" else None,
    )


def fetch_annotation_graph_ch(
    *,
    analytics,
    project_id: str,
    filters: list[dict[str, Any]],
    interval: str,
    req_data_config: dict[str, Any],
    observe_type: str,
    timeout_ms: int = GRAPH_READ_TIMEOUT_MS,
) -> dict[str, Any]:
    label_id = req_data_config.get("id")
    if not label_id:
        raise ValueError("Annotation label ID is required")
    # Annotation labels can be project-local or org/shared labels that are
    # only connected to a project through Score rows. Use the same score-backed
    # lookup as list/filter config so graph metrics work for rendered labels.
    label = get_annotation_labels_for_project(project_id).get(id=label_id)
    builder = AnnotationGraphQueryBuilder(
        project_id=str(project_id),
        annotation_label_id=str(label_id),
        annotation_name=label.name,
        filters=filters,
        interval=interval,
        output_type=annotation_output_type(label, req_data_config.get("output_type")),
        value=req_data_config.get("value"),
        observe_type=observe_type,
    )
    query, params = builder.build()
    result = analytics.execute_ch_query(
        query,
        params,
        timeout_ms=timeout_ms,
        settings=GRAPH_READ_SETTINGS,
    )
    return builder.format_result(result.data, result.columns or [])
