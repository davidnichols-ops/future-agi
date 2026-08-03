"""
Span Attribute Discovery APIs for ClickHouse.

Endpoints:
1. GET /api/traces/span-attribute-keys/ - Discover all attribute keys for a project
2. GET /api/traces/span-attribute-values/ - Get top values for an attribute key
3. GET /api/traces/span-attribute-detail/<key>/ - Full detail for a specific attribute key
"""

from dataclasses import asdict

import structlog
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from tfc.utils.api_contracts import validated_request
from tfc.utils.api_serializers import ApiTextErrorResponseSerializer
from tfc.utils.general_methods import GeneralMethods
from tracer.serializers.span_attributes import (
    SpanAttributeDetailQuerySerializer,
    SpanAttributeDetailResponseSerializer,
    SpanAttributeKeysResponseSerializer,
    SpanAttributeProjectQuerySerializer,
    SpanAttributeValuesQuerySerializer,
    SpanAttributeValuesResponseSerializer,
)
from tracer.services.clickhouse.attribute_reads import (
    AttributeDetailRead,
    AttributeReadMetadata,
    AttributeReadSelector,
)
from tracer.services.clickhouse.read_budget import (
    is_clickhouse_query_error,
    is_read_budget_error,
)
from tracer.utils.workspace_scope import project_queryset_for_request

logger = structlog.get_logger(__name__)

_DETAIL_TOP_VALUES = 100

ERROR_RESPONSES = {
    400: ApiTextErrorResponseSerializer,
    404: ApiTextErrorResponseSerializer,
    500: ApiTextErrorResponseSerializer,
    503: ApiTextErrorResponseSerializer,
}


def _project_is_in_request_scope(request, project_id: str) -> bool:
    """Run the only PostgreSQL query allowed by these telemetry endpoints."""

    return project_queryset_for_request(request).filter(id=project_id).exists()


def _attribute_error_code(exc: Exception) -> str:
    return "read_budget_exceeded" if is_read_budget_error(exc) else "query_failed"


def _is_expected_attribute_read_failure(exc: Exception) -> bool:
    """Return whether an attribute read may safely become degraded metadata.

    Only driver-typed ClickHouse failures and explicit read-budget exhaustion
    are operational failures.  Arbitrary ``Exception`` values are programming
    defects; turning those into an empty successful picker hid regressions in
    production and made a broken attribute compiler look like "no values".
    """

    return is_read_budget_error(exc) or is_clickhouse_query_error(exc)


class SpanAttributeKeysView(APIView):
    """
    Discover all span attribute keys for a project.

    Returns every distinct key across the string, number, and boolean attribute
    maps together with its inferred type and occurrence count.

    GET /api/traces/span-attribute-keys/?project_id=<uuid>
    """

    permission_classes = [IsAuthenticated]
    _gm = GeneralMethods()

    @validated_request(
        query_serializer=SpanAttributeProjectQuerySerializer,
        responses={200: SpanAttributeKeysResponseSerializer, **ERROR_RESPONSES},
    )
    def get(self, request, *args, **kwargs):
        project_id = ""
        selector: AttributeReadSelector | None = None
        try:
            project_id = str(request.validated_query_data["project_id"])
            exact_key = request.validated_query_data.get("q")
            selector = AttributeReadSelector(
                typed_only=True,
                json_attribute_mode="arrays",
            )
            if not _project_is_in_request_scope(request, project_id):
                return self._gm.not_found("Project not found")
            read = selector.discover_keys([project_id], exact_key=exact_key)
            return Response(
                {
                    "result": [asdict(row) for row in read.rows],
                    **read.metadata.public_payload(),
                },
                status=200,
            )
        except Exception as exc:
            if not _is_expected_attribute_read_failure(exc):
                logger.exception(
                    "span_attribute_keys_programming_error",
                    project_id=project_id,
                    error_type=type(exc).__name__,
                )
                return self._gm.internal_server_error_response(
                    "Span attribute keys could not be loaded"
                )
            if selector is None:
                logger.warning(
                    "span_attribute_keys_unavailable",
                    project_id=project_id,
                    error_type=type(exc).__name__,
                )
                return self._gm.custom_error_response(
                    503,
                    "Span attribute keys are temporarily unavailable. Please retry.",
                    code="service_unavailable",
                )
            logger.warning(
                "span_attribute_keys_failed",
                project_id=project_id,
                error_type=type(exc).__name__,
            )
            return Response(
                {
                    "result": [],
                    **selector.degraded_metadata(
                        _attribute_error_code(exc)
                    ).public_payload(),
                },
                status=200,
            )


class SpanAttributeValuesView(APIView):
    """
    Get top values for a specific span attribute key.

    Returns the most frequent values for the given string attribute key,
    with optional prefix search filtering.

    GET /api/traces/span-attribute-values/?project_id=<uuid>&key=<attr_key>[&q=<search>][&limit=50]
    """

    permission_classes = [IsAuthenticated]
    _gm = GeneralMethods()

    @validated_request(
        query_serializer=SpanAttributeValuesQuerySerializer,
        responses={200: SpanAttributeValuesResponseSerializer, **ERROR_RESPONSES},
    )
    def get(self, request, *args, **kwargs):
        project_id = ""
        key = ""
        selector: AttributeReadSelector | None = None
        try:
            query_params = request.validated_query_data
            project_id = str(query_params["project_id"])
            key = query_params["key"]
            q = query_params.get("q")
            limit = query_params.get("limit", 50)
            selector = AttributeReadSelector(
                typed_only=True,
                json_attribute_mode="arrays",
            )
            if not _project_is_in_request_scope(request, project_id):
                return self._gm.not_found("Project not found")
            read = selector.read_values([project_id], key, search=q, max_values=limit)
            return Response(
                {
                    "result": [asdict(row) for row in read.rows],
                    **read.metadata.public_payload(),
                },
                status=200,
            )
        except Exception as exc:
            if not _is_expected_attribute_read_failure(exc):
                logger.exception(
                    "span_attribute_values_programming_error",
                    project_id=project_id,
                    key=key,
                    error_type=type(exc).__name__,
                )
                return self._gm.internal_server_error_response(
                    "Span attribute values could not be loaded"
                )
            if selector is None:
                logger.warning(
                    "span_attribute_values_unavailable",
                    project_id=project_id,
                    key=key,
                    error_type=type(exc).__name__,
                )
                return self._gm.custom_error_response(
                    503,
                    "Span attribute values are temporarily unavailable. Please retry.",
                    code="service_unavailable",
                )
            logger.warning(
                "span_attribute_values_failed",
                project_id=project_id,
                key=key,
                error_type=type(exc).__name__,
            )
            return Response(
                {
                    "result": [],
                    **selector.degraded_metadata(
                        _attribute_error_code(exc)
                    ).public_payload(),
                },
                status=200,
            )


class SpanAttributeDetailView(APIView):
    """
    Full detail for a specific span attribute key.

    Determines the attribute type by probing which map contains the key, then
    returns type-appropriate statistics:
      - string: top values with percentages
      - number: min, max, avg, p50, p95
      - boolean: true/false distribution

    GET /api/traces/span-attribute-detail/?project_id=<uuid>&key=<attr_key>
    """

    permission_classes = [IsAuthenticated]
    _gm = GeneralMethods()

    @validated_request(
        query_serializer=SpanAttributeDetailQuerySerializer,
        responses={200: SpanAttributeDetailResponseSerializer, **ERROR_RESPONSES},
    )
    def get(self, request, *args, **kwargs):
        project_id = ""
        key = ""
        selector: AttributeReadSelector | None = None
        try:
            query_params = request.validated_query_data
            project_id = str(query_params["project_id"])
            key = query_params["key"]
            selector = AttributeReadSelector(
                typed_only=True,
                json_attribute_mode="arrays",
            )
            if not _project_is_in_request_scope(request, project_id):
                return self._gm.not_found("Project not found")
            read = selector.read_detail([project_id], key)
            if read.attribute_type is None and read.metadata.query_complete:
                return self._gm.not_found(f"Attribute key '{key}' not found in project")
            if read.attribute_type is None:
                return Response(
                    self._empty_degraded_payload(key, read.metadata),
                    status=200,
                )
            return Response(self._detail_payload(key, read), status=200)
        except Exception as exc:
            if not _is_expected_attribute_read_failure(exc):
                logger.exception(
                    "span_attribute_detail_programming_error",
                    project_id=project_id,
                    key=key,
                    error_type=type(exc).__name__,
                )
                return self._gm.internal_server_error_response(
                    "Span attribute details could not be loaded"
                )
            if selector is None:
                logger.warning(
                    "span_attribute_detail_unavailable",
                    project_id=project_id,
                    key=key,
                    error_type=type(exc).__name__,
                )
                return self._gm.custom_error_response(
                    503,
                    "Span attribute details are temporarily unavailable. Please retry.",
                    code="service_unavailable",
                )
            logger.warning(
                "span_attribute_detail_failed",
                project_id=project_id,
                key=key,
                error_type=type(exc).__name__,
            )
            return Response(
                self._empty_degraded_payload(
                    key,
                    selector.degraded_metadata(_attribute_error_code(exc)),
                ),
                status=200,
            )

    @staticmethod
    def _empty_degraded_payload(
        key: str,
        metadata: AttributeReadMetadata,
    ) -> dict:
        """Keep required legacy fields while making incompleteness explicit."""

        return {
            "key": key,
            # The established response requires a concrete scalar type. No
            # type claim is actionable while query_complete is false; string
            # preserves the old response shape for existing clients.
            "type": "string",
            "count": 0,
            "unique_values": 0,
            "top_values": [],
            **metadata.public_payload(),
        }

    @staticmethod
    def _weighted_quantile(
        values: list[tuple[float, int]],
        quantile: float,
    ) -> float | None:
        total = sum(count for _, count in values)
        if total <= 0:
            return None
        target = max(1, int(total * quantile + 0.999999999))
        cumulative = 0
        for value, count in sorted(values):
            cumulative += count
            if cumulative >= target:
                return value
        return max(value for value, _ in values)

    @classmethod
    def _detail_payload(cls, key: str, read: AttributeDetailRead) -> dict:
        attribute_type = read.attribute_type
        assert attribute_type is not None
        total_count = sum(row.count for row in read.rows)
        payload: dict = {
            "key": key,
            "type": attribute_type,
            "count": total_count,
            **read.metadata.public_payload(),
        }
        if attribute_type == "number":
            weighted_values = [
                (float(row.value), row.count)
                for row in read.rows
                if isinstance(row.value, (int, float))
                and not isinstance(row.value, bool)
            ]
            weighted_sum = sum(value * count for value, count in weighted_values)
            payload.update(
                {
                    "min": min((value for value, _ in weighted_values), default=None),
                    "max": max((value for value, _ in weighted_values), default=None),
                    "avg": (
                        round(weighted_sum / total_count, 4)
                        if total_count > 0
                        else None
                    ),
                    "p50": cls._rounded_quantile(weighted_values, 0.50),
                    "p95": cls._rounded_quantile(weighted_values, 0.95),
                }
            )
            return payload

        payload["unique_values"] = len(read.rows)
        payload["top_values"] = [
            {
                "value": row.value,
                "count": row.count,
                "percentage": (
                    round(row.count / total_count * 100, 1) if total_count > 0 else 0
                ),
            }
            for row in read.rows[:_DETAIL_TOP_VALUES]
        ]
        return payload

    @classmethod
    def _rounded_quantile(
        cls,
        values: list[tuple[float, int]],
        quantile: float,
    ) -> float | None:
        value = cls._weighted_quantile(values, quantile)
        return round(value, 4) if value is not None else None
