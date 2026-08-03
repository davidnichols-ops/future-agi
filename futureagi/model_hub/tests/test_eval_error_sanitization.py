from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from rest_framework import status

from model_hub.models.choices import OwnerChoices
from model_hub.models.evals_metric import EvalTemplate
from model_hub.selectors.eval_usage import (
    EvalUsageRead,
    EvalUsageReadCompleteness,
    EvalUsageReadError,
    EvalUsageReadErrorCode,
)
from model_hub.views import separate_evals


def _usage_template(organization, workspace):
    return EvalTemplate.no_workspace_objects.create(
        name=f"bounded-usage-{uuid.uuid4().hex[:8]}",
        organization=organization,
        workspace=workspace,
        owner=OwnerChoices.USER.value,
        config={"output": "Pass/Fail", "eval_type_id": "AgentEvaluator"},
        eval_tags=["llm"],
        criteria="Check {{response}}",
        model="turing_large",
        visible_ui=True,
    )


@pytest.mark.django_db
def test_eval_logs_table_internal_error_is_sanitized(auth_client, monkeypatch):
    """ClickHouse/database internals never cross the eval-settings API boundary."""

    private_error = "Code: 159. DB::Exception: private query and stack"

    def fail_access_check(*_args, **_kwargs):
        raise RuntimeError(private_error)

    # The OSS test lane intentionally has no EE APICallLog model. A non-None
    # placeholder reaches the same guarded endpoint branch without requiring
    # any external table or write.
    monkeypatch.setattr(separate_evals, "APICallLog", SimpleNamespace())
    monkeypatch.setattr(
        separate_evals,
        "_get_accessible_eval_template",
        fail_access_check,
    )

    response = auth_client.get(
        "/model-hub/get-eval-logs-details",
        {
            "eval_template_id": str(uuid.uuid4()),
            "source": "eval_playground",
            "current_page_index": 0,
            "page_size": 10,
        },
    )

    rendered = response.content.decode()
    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Unable to load evaluation logs. Please try again later." in rendered
    assert private_error not in rendered


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("failure_code", "response_code"),
    [
        (
            EvalUsageReadErrorCode.DEADLINE_EXCEEDED,
            "eval_usage_deadline_exceeded",
        ),
        (EvalUsageReadErrorCode.QUERY_FAILED, "eval_usage_query_failed"),
    ],
)
def test_eval_usage_typed_failure_is_sanitized_at_api_boundary(
    auth_client,
    organization,
    workspace,
    monkeypatch,
    failure_code,
    response_code,
):
    template = _usage_template(organization, workspace)
    monkeypatch.setattr(separate_evals, "APICallLog", SimpleNamespace())
    monkeypatch.setattr(separate_evals, "is_clickhouse_enabled", lambda: True)

    def fail_bounded_read(**_kwargs):
        raise EvalUsageReadError(
            failure_code,
            operations=("total",),
        )

    monkeypatch.setattr(separate_evals, "read_eval_usage", fail_bounded_read)

    response = auth_client.get(
        f"/model-hub/eval-templates/{template.id}/usage/",
        {"page": 0, "page_size": 25, "period": "30d"},
    )

    body = response.json()
    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert body["code"] == response_code
    assert body["result"] == (
        "Evaluation usage could not be loaded. Please try again later."
    )
    assert "stats" not in str(body)


@pytest.mark.django_db
def test_eval_usage_clickhouse_response_preserves_required_total_runs(
    auth_client,
    organization,
    workspace,
    monkeypatch,
):
    template = _usage_template(organization, workspace)
    monkeypatch.setattr(separate_evals, "APICallLog", SimpleNamespace())
    monkeypatch.setattr(separate_evals, "is_clickhouse_enabled", lambda: True)
    monkeypatch.setattr(
        separate_evals,
        "read_eval_usage",
        lambda **_kwargs: EvalUsageRead(
            total_runs=0,
            runs_period=0,
            success_count=0,
            error_count=0,
            chart=[],
            logs=[],
            completeness=EvalUsageReadCompleteness.COMPLETE,
            unavailable_fields=(),
        ),
    )

    response = auth_client.get(
        f"/model-hub/eval-templates/{template.id}/usage/",
        {"page": 0, "page_size": 25, "period": "30d"},
    )

    result = response.json()["result"]
    assert response.status_code == status.HTTP_200_OK
    assert result["completeness"] == "complete"
    assert result["unavailable_fields"] == []
    assert result["stats"]["total_runs"] == 0


@pytest.mark.django_db
def test_eval_usage_programming_defect_re_raises_through_api_boundary(
    auth_client,
    organization,
    workspace,
    monkeypatch,
):
    template = _usage_template(organization, workspace)
    monkeypatch.setattr(separate_evals, "APICallLog", SimpleNamespace())
    monkeypatch.setattr(separate_evals, "is_clickhouse_enabled", lambda: True)

    def fail_with_bug(**_kwargs):
        raise KeyError("eval usage application bug")

    monkeypatch.setattr(separate_evals, "read_eval_usage", fail_with_bug)

    with pytest.raises(KeyError, match="eval usage application bug"):
        auth_client.get(
            f"/model-hub/eval-templates/{template.id}/usage/",
            {"page": 0, "page_size": 25, "period": "30d"},
        )
