"""Run or inspect the isolated unified property catalog in DEV only."""

from __future__ import annotations

from argparse import ArgumentParser
from typing import Any

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils.module_loading import import_string

from tracer.services.clickhouse.v2.property_catalog.codec import canonical_json
from tracer.services.clickhouse.v2.property_catalog.dev_rollout import (
    ConfiguredDevRolloutRuntime,
    DevRolloutError,
    DevRolloutRequest,
    configured_dev_rollout_request,
    run_configured_dev_rollout,
)
from tracer.services.clickhouse.v2.property_catalog.dev_runtime import (
    CHECKED_IN_DEV_RUNTIME_FACTORY_PATH,
    require_checked_in_property_catalog_dev_runtime,
)

_RUNTIME_FACTORY_SETTING = "PROPERTY_CATALOG_DEV_RUNTIME_FACTORY"


class Command(BaseCommand):
    help = (
        "Plan, inspect, or execute the clean six-table unified property catalog "
        "inside one exact isolated DEV database."
    )

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("--organization-id")
        parser.add_argument("--workspace-id")
        parser.add_argument("--environment")
        parser.add_argument("--cloud-deployment")
        parser.add_argument("--dev-identity")
        parser.add_argument("--source-database")
        parser.add_argument("--target-database")
        parser.add_argument("--ack", dest="acknowledgement")
        parser.add_argument("--initial-backfill-wall-ms", type=int)
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--status", action="store_true")
        mode.add_argument("--execute", action="store_true")

    def handle(self, *args: Any, **options: Any) -> str:
        try:
            request = _request(options)
            runtime = _runtime(request) if request.execute or request.status else None
            result = run_configured_dev_rollout(request=request, runtime=runtime)
        except (DevRolloutError, TypeError, ValueError, ImportError) as exc:
            raise CommandError(str(exc)) from exc
        output = canonical_json(result.as_dict(), max_bytes=4 * 1024 * 1024)
        return output


def _request(options: dict[str, Any]) -> DevRolloutRequest:
    organization_id = options.get("organization_id") or getattr(
        settings, "PROPERTY_CATALOG_DEV_ORGANIZATION_ID", ""
    )
    workspace_id = options.get("workspace_id") or getattr(
        settings, "PROPERTY_CATALOG_DEV_WORKSPACE_ID", ""
    )
    return configured_dev_rollout_request(
        organization_id=str(organization_id),
        workspace_id=str(workspace_id),
        settings_object=settings,
        execute=bool(options.get("execute")),
        status=bool(options.get("status")),
        initial_backfill_wall_ms=options.get("initial_backfill_wall_ms"),
        overrides={
            "acknowledgement": options.get("acknowledgement"),
            "cloud_deployment": options.get("cloud_deployment"),
            "dev_identity": options.get("dev_identity"),
            "environment": options.get("environment"),
            "source_database": options.get("source_database"),
            "target_database": options.get("target_database"),
        },
    )


def _runtime(request: DevRolloutRequest) -> ConfiguredDevRolloutRuntime:
    dotted_path = getattr(settings, _RUNTIME_FACTORY_SETTING, "")
    if not isinstance(dotted_path, str) or not dotted_path:
        raise DevRolloutError(
            f"{_RUNTIME_FACTORY_SETTING} must name the reviewed DEV runtime factory"
        )
    if dotted_path != CHECKED_IN_DEV_RUNTIME_FACTORY_PATH:
        raise DevRolloutError(
            f"{_RUNTIME_FACTORY_SETTING} must equal the reviewed checked-in factory"
        )
    factory = import_string(dotted_path)
    if not callable(factory):
        raise DevRolloutError("configured DEV runtime factory is not callable")
    runtime = factory(request)
    require_checked_in_property_catalog_dev_runtime(runtime)
    required = (
        "activate",
        "apply_schema",
        "backfill",
        "postgres_adapters",
        "postgres_reconciler",
        "postgres_request_factory",
        "postgres_snapshot_guard",
        "qualify",
        "reconcile_workspace",
        "reconcile_non_postgres",
        "status",
        "verify_schema",
    )
    if any(not callable(getattr(runtime, name, None)) for name in required):
        raise DevRolloutError("configured DEV runtime is missing a required stage")
    return runtime
