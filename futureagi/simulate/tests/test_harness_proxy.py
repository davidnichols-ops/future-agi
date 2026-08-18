"""
API tests for HarnessProxyView.

Tests cover:
- Auth gating (anonymous rejected)
- Response enrichment with platform run_test/execution ids
- Platform ids stripped from the forwarded body and persisted to HarnessSessionLink
- Harness error/timeout passthrough (409, 502)
- Path traversal refusal
- SSE Accept header not triggering DRF content negotiation 406
- Non-POST requests to streaming paths (say/run) rejected with 405 before any upstream call
"""

import json
from unittest.mock import patch

import httpx
from django.urls import reverse

from simulate.models import HarnessSessionLink

STATUS_PAYLOAD = {"session": {"id": "abc123"}, "stage": "build", "busy": False}


def _url(path):
    return reverse("harness-proxy", kwargs={"path": path})


def test_anonymous_requests_are_rejected(api_client):
    assert api_client.get(_url("status")).status_code in (401, 403)


@patch("simulate.views.harness_proxy.httpx.request")
def test_status_is_enriched_with_platform_ids(mock_request, auth_client):
    HarnessSessionLink.objects.create(
        session_id="abc123", run_test_id="rt-1", execution_id="ex-1"
    )
    mock_request.return_value = httpx.Response(
        200, json=STATUS_PAYLOAD, headers={"content-type": "application/json"}
    )
    answered = auth_client.get(_url("status")).json()
    assert answered["run_test_id"] == "rt-1"
    assert answered["execution_id"] == "ex-1"


@patch("simulate.views.harness_proxy.httpx.request")
def test_session_creation_strips_and_stores_platform_ids(mock_request, auth_client):
    mock_request.return_value = httpx.Response(
        200, json=STATUS_PAYLOAD, headers={"content-type": "application/json"}
    )
    auth_client.post(
        _url("sessions"),
        data=json.dumps({"agent": "support", "run_test_id": "rt-9", "execution_id": "ex-9"}),
        content_type="application/json",
    )
    forwarded = mock_request.call_args.kwargs["json"]
    assert "run_test_id" not in forwarded and forwarded == {"agent": "support"}
    link = HarnessSessionLink.objects.get(session_id="abc123")
    assert (link.run_test_id, link.execution_id) == ("rt-9", "ex-9")


@patch("simulate.views.harness_proxy.httpx.request")
def test_harness_errors_pass_through(mock_request, auth_client):
    mock_request.return_value = httpx.Response(
        409,
        json={"error": "still working on the last thing"},
        headers={"content-type": "application/json"},
    )
    answered = auth_client.post(_url("stage"), data={}, format="json")
    assert answered.status_code == 409
    assert "run_test_id" not in answered.json()


@patch("simulate.views.harness_proxy.httpx.request")
def test_unreachable_harness_maps_to_502(mock_request, auth_client):
    mock_request.side_effect = httpx.ConnectError("boom")
    assert auth_client.get(_url("status")).status_code == 502


def test_traversal_is_refused(auth_client):
    assert auth_client.get(_url("..%2Fadmin")).status_code == 404


@patch("simulate.views.harness_proxy.httpx.request")
def test_sse_accept_header_is_not_rejected(mock_request, auth_client):
    mock_request.return_value = httpx.Response(
        200, json=STATUS_PAYLOAD, headers={"content-type": "application/json"}
    )
    answered = auth_client.get(_url("status"), HTTP_ACCEPT="text/event-stream")
    assert answered.status_code == 200


@patch("simulate.views.harness_proxy.httpx.request")
def test_non_post_to_streaming_paths_is_405(mock_request, auth_client):
    answered = auth_client.get(_url("say"))
    assert answered.status_code == 405
    mock_request.assert_not_called()
