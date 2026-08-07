"""
Span Attribute Discovery APIs for ClickHouse.

Endpoints:
1. GET /api/traces/span-attribute-keys/ - Discover all attribute keys for a project
2. GET /api/traces/span-attribute-values/ - Get top values for an attribute key
3. GET /api/traces/span-attribute-detail/<key>/ - Full detail for a specific attribute key
"""

from dataclasses import asdict
from datetime import UTC, datetime, timedelta

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
from tracer.services.clickhouse.attribute_cursor_state import (
    AttributeCursorStateError,
    load_attribute_cursor_seen_state,
    persist_attribute_cursor_seen_state,
)
from tracer.services.clickhouse.attribute_reads import AttributeReadSelector
from tracer.services.clickhouse.list_cursor import (
    ListCursorError,
    cursor_scope_for_request,
    decode_list_cursor,
    encode_list_cursor,
)
from tracer.services.clickhouse.read_budget import (
    is_clickhouse_query_error,
    is_read_budget_error,
)
from tracer.services.exact_aggregation_cache import read_or_schedule_exact_snapshot
from tracer.utils.workspace_scope import project_queryset_for_request

logger = structlog.get_logger(__name__)

ERROR_RESPONSES = {
    400: ApiTextErrorResponseSerializer,
    404: ApiTextErrorResponseSerializer,
    500: ApiTextErrorResponseSerializer,
    503: ApiTextErrorResponseSerializer,
}

# Attribute-name discovery is project metadata, not a preview of the task or
# dashboard time window.  Freeze cursor walks at the earliest timestamp the
# spans contract accepts so pagination can reach every retained project row.
# Per-request candidate/query ceilings in ``AttributeReadSelector`` keep each
# continuation bounded; retention jobs decide which rows still exist.
# The table is partitioned by ``toDate(start_time)``. Although DateTime64 can
# represent pre-1970 instants, Date cannot; a 1900 predicate can be folded into
# a wrapped partition bound and incorrectly prune current data. No telemetry
# predates Unix time, so epoch is the earliest lossless retained-data bound.
SPAN_ATTRIBUTE_RETAINED_DATA_START = datetime(1970, 1, 1, tzinfo=UTC)


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


def _attribute_key_payload(row) -> dict:
    payload = asdict(row)
    if not payload.get("types"):
        payload.pop("types", None)
    # Discovery is deliberately bounded. ``count`` is useful for ordering but
    # is not an exact tenant-wide total unless a future exact endpoint says so.
    payload["count_exact"] = False
    return payload


class SpanAttributeKeysView(APIView):
    """
    Discover span attribute keys for a project.

    Cursor mode walks retained project data newest-first in bounded pages;
    exact ``q`` lookup remains available for direct key discovery.
    The no-page-size form is retained for older clients.

    GET /api/traces/span-attribute-keys/?project_id=<uuid>&page_size=10
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
            query_params = request.validated_query_data
            exact_key = query_params.get("q")
            page_size = query_params.get("page_size")
            cursor_token = query_params.get("cursor")
            selector = AttributeReadSelector(
                typed_only=True,
                json_attribute_mode="structured",
            )
            if not _project_is_in_request_scope(request, project_id):
                return self._gm.not_found("Project not found")

            if page_size is not None:
                page_size = int(page_size)
                project_ids = [project_id]
                cursor_scope = cursor_scope_for_request(
                    request,
                    project_ids=project_ids,
                )
                cursor_query = {
                    "project_id": project_id,
                    "mode": "recent_attribute_keys",
                }
                if exact_key is not None:
                    # Signed cursor and server-side seen state are scoped to
                    # the normalized exact key. A continuation for one search
                    # can therefore never be replayed under another key.
                    cursor_query["q"] = exact_key
                if cursor_token:
                    cursor_state = decode_list_cursor(
                        cursor_token,
                        resource="span_attribute_keys",
                        scope=cursor_scope,
                        query=cursor_query,
                        page_size=page_size,
                    )
                    if len(cursor_state.order) not in {5, 6}:
                        raise ListCursorError(
                            "invalid_cursor",
                            "The continuation cursor is invalid.",
                        )
                    (
                        segment_end,
                        raw_before_identity,
                        raw_resume_identity,
                        resume_key_offset,
                        seen_reference,
                    ) = cursor_state.order[:5]
                    raw_segment_start = (
                        cursor_state.order[5] if len(cursor_state.order) == 6 else None
                    )
                    if (
                        not isinstance(segment_end, datetime)
                        or not isinstance(raw_before_identity, tuple)
                        or len(raw_before_identity) not in {0, 4}
                        or not isinstance(raw_resume_identity, tuple)
                        or len(raw_resume_identity) not in {0, 4}
                        or (raw_before_identity and raw_resume_identity)
                        or not isinstance(resume_key_offset, int)
                        or resume_key_offset < 0
                        or (
                            raw_segment_start is not None
                            and not isinstance(raw_segment_start, datetime)
                        )
                        or (
                            raw_segment_start is not None
                            and not (raw_before_identity or raw_resume_identity)
                        )
                    ):
                        raise ListCursorError(
                            "invalid_cursor",
                            "The continuation cursor is invalid.",
                        )

                    def restore_identity(raw_identity):
                        if not raw_identity:
                            return None
                        if not all(
                            isinstance(value, str) for value in raw_identity[:3]
                        ) or not isinstance(raw_identity[3], datetime):
                            raise ListCursorError(
                                "invalid_cursor",
                                "The continuation cursor is invalid.",
                            )
                        return raw_identity

                    before_identity = restore_identity(raw_before_identity)
                    resume_identity = restore_identity(raw_resume_identity)
                    segment_start = raw_segment_start
                    window_start = cursor_state.window_start
                    window_end = cursor_state.window_end
                else:
                    window_end = datetime.now(UTC)
                    retained_start = selector.retained_window_start(
                        project_ids,
                        window_end=window_end,
                    )
                    window_start = (
                        max(
                            SPAN_ATTRIBUTE_RETAINED_DATA_START,
                            retained_start,
                        )
                        if retained_start is not None
                        else window_end - timedelta(microseconds=1)
                    )
                    segment_end = window_end
                    segment_start = None
                    before_identity = None
                    resume_identity = None
                    resume_key_offset = 0
                    seen_reference = ()

                state_binding = {
                    "scope": cursor_scope,
                    "query": cursor_query,
                    "page_size": page_size,
                    "window_start": window_start,
                    "window_end": window_end,
                }
                seen_state = load_attribute_cursor_seen_state(
                    seen_reference,
                    resource="span_attribute_keys",
                    binding=state_binding,
                    validate_digest=lambda value: (
                        len(value) == 32
                        and all(char in "0123456789abcdef" for char in value)
                    ),
                )
                if cursor_token and cursor_state.seen_rows != len(seen_state.digests):
                    raise ListCursorError(
                        "invalid_cursor",
                        "The continuation cursor is invalid.",
                    )

                page_read = selector.read_key_cursor_page(
                    project_ids,
                    page_size=page_size,
                    window_start=window_start,
                    window_end=window_end,
                    segment_end=segment_end,
                    segment_start=segment_start,
                    before_identity=before_identity,
                    resume_identity=resume_identity,
                    resume_key_offset=resume_key_offset,
                    seen_key_digests=seen_state.digests,
                    exact_key=exact_key,
                    continue_operation=not bool(cursor_token),
                )
                next_cursor = None
                published_has_more = page_read.has_more
                published_browse_status = page_read.browse_status
                if published_has_more:
                    appended_digests = page_read.seen_key_digests[
                        len(seen_state.digests) :
                    ]
                    seen_reference = persist_attribute_cursor_seen_state(
                        seen_state,
                        appended_digests,
                        resource="span_attribute_keys",
                        binding=state_binding,
                        validate_digest=lambda value: (
                            len(value) == 32
                            and all(char in "0123456789abcdef" for char in value)
                        ),
                    )
                    next_order = (
                        page_read.next_segment_end,
                        page_read.next_before_identity or (),
                        page_read.next_resume_identity or (),
                        page_read.next_resume_key_offset,
                        seen_reference,
                    )
                    if page_read.next_segment_start is not None:
                        next_order = (*next_order, page_read.next_segment_start)
                    next_cursor = encode_list_cursor(
                        resource="span_attribute_keys",
                        scope=cursor_scope,
                        query=cursor_query,
                        page_size=page_size,
                        window_start=window_start,
                        window_end=window_end,
                        order=next_order,
                        seen_rows=len(page_read.seen_key_digests),
                    )
                return Response(
                    {
                        # Cursor browse counts only describe occurrences inside
                        # the bounded physical prefix used to discover this
                        # suggestion.  Never present them as exact tenant-wide
                        # span totals.
                        "result": [
                            _attribute_key_payload(row) for row in page_read.rows
                        ],
                        **page_read.metadata.public_payload(),
                        "has_more": published_has_more,
                        "next_cursor": next_cursor,
                        # Preserve the rolling-deploy response enum. Despite
                        # this legacy label, the frozen cursor window now spans
                        # all retained project data, not a UI date range.
                        "browse_mode": "recent_suggestions",
                        "browse_status": published_browse_status,
                        **(
                            {
                                "lookup_mode": "exact",
                                "exact_match": any(
                                    row.key == exact_key for row in page_read.rows
                                ),
                            }
                            if exact_key is not None
                            else {}
                        ),
                    },
                    status=200,
                )

            # The retained-data cursor above is the exhaustive path. Keep this
            # compatibility exact-q endpoint on its production-qualified
            # adaptive windows: one unsegmented 1970-to-now Map probe scanned
            # hundreds of millions of rows on the incident tenant and could
            # recreate the original 503. UI consumers paginate the retained
            # catalog and filter those verified typed names locally.
            read = selector.discover_keys([project_id], exact_key=exact_key)
            return Response(
                {
                    "result": [_attribute_key_payload(row) for row in read.rows],
                    **read.metadata.public_payload(),
                    **(
                        {
                            "lookup_mode": "exact",
                            "exact_match": any(
                                row.key == exact_key for row in read.rows
                            ),
                        }
                        if exact_key is not None
                        else {}
                    ),
                },
                status=200,
            )
        except AttributeCursorStateError as exc:
            if exc.code == "cursor_state_unavailable":
                return self._gm.custom_error_response(
                    503,
                    str(exc),
                    code="service_unavailable",
                )
            return self._gm.custom_error_response(400, str(exc), code=exc.code)
        except ListCursorError as exc:
            return self._gm.custom_error_response(
                400,
                str(exc),
                code=exc.code,
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
    Serve the last complete exact attribute snapshot and refresh out of band.

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
        try:
            query_params = request.validated_query_data
            project_id = str(query_params["project_id"])
            key = query_params["key"]
            if not _project_is_in_request_scope(request, project_id):
                return self._gm.not_found("Project not found")

            identity = {
                "workspace_id": str(request.workspace.id),
                "project_id": project_id,
                "attribute_key": key,
                "horizon_days": 365,
            }
            payload = read_or_schedule_exact_snapshot(
                "attribute-detail",
                identity,
                refresh=bool(query_params.get("refresh", False)),
                pending_payload={
                    "key": key,
                    "type": None,
                    "count": 0,
                    "unique_values": 0,
                    "top_values": [],
                    "query_complete": False,
                    "query_status": "pending",
                    "query_sampled": False,
                },
            )
            return Response(payload, status=200)
        except Exception as exc:
            logger.exception(
                "span_attribute_detail_programming_error",
                project_id=project_id,
                key=key,
                error_type=type(exc).__name__,
            )
            return self._gm.internal_server_error_response(
                "Span attribute details could not be loaded"
            )
