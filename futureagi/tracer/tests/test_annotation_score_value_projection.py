"""No-database contracts for the exact annotation value projection."""

from __future__ import annotations

import importlib
from contextlib import nullcontext
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tracer.services import annotation_label_source as source


class _Cursor:
    def __init__(self, rows):
        self.rows = list(rows)
        self.query = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchall(self):
        return self.rows


@pytest.mark.unit
def test_projection_reader_uses_bounded_indexed_keyset_and_revision(monkeypatch):
    project_id = str(uuid4())
    label_id = str(uuid4())
    cursor = _Cursor(
        [
            (True, 1, 17, False, "Alpha", "alpha", "a" * 64),
            (True, 1, 17, False, "Beta", "beta", "b" * 64),
        ]
    )
    monkeypatch.setattr(
        source,
        "connection",
        SimpleNamespace(cursor=lambda: cursor),
    )

    page = source.AnnotationLabelScoresProjectPG().categorical_value_page_for_label(
        label_id,
        [project_id],
        page_size=1,
        search=r"50%_done\\x",
        after=("aardvark", "0" * 64),
        expected_revision="1:17",
        excluded_values=("Configured",),
    )

    assert [row.value for row in page.rows] == ["Alpha"]
    assert page.revision == "1:17"
    assert page.has_more is True
    compact_sql = " ".join(cursor.query.split())
    assert "model_hub_annotation_value_vocab" in compact_sql
    assert "model_hub_annotation_value_status" in compact_sql
    assert "status.organization_id = project.organization_id" in compact_sql
    assert "status.singleton_id" not in compact_sql
    assert "SELECT unnest(%s::uuid[]) AS project_id" in compact_sql
    assert "v.tracer_project_id = requested.project_id" in compact_sql
    assert "CROSS JOIN LATERAL" in compact_sql
    assert "HAVING count(*) > %s" in compact_sql
    assert "v.value_search LIKE %s" in compact_sql
    assert "(v.value_sort_prefix, v.value_digest) >" in compact_sql
    assert "LIMIT %s" in compact_sql
    assert "FROM model_hub_score" not in compact_sql
    assert cursor.params[-1] == 2
    assert r"%50\%\_done\\\\x%" in cursor.params


@pytest.mark.unit
def test_projection_reader_fails_closed_when_status_is_not_ready(monkeypatch):
    cursor = _Cursor([(False, 1, 9, False, None, None, None)])
    monkeypatch.setattr(
        source,
        "connection",
        SimpleNamespace(cursor=lambda: cursor),
    )

    with pytest.raises(source.AnnotationScoreReadUnavailable, match="not ready"):
        source.AnnotationLabelScoresProjectPG().categorical_value_page_for_label(
            str(uuid4()),
            [str(uuid4())],
            page_size=10,
        )


@pytest.mark.unit
def test_projection_reader_rejects_unbounded_project_batches():
    with pytest.raises(ValueError, match="cannot exceed 64 projects"):
        source.AnnotationLabelScoresProjectPG().categorical_value_page_for_label(
            str(uuid4()),
            [str(uuid4()) for _index in range(65)],
            page_size=10,
        )


@pytest.mark.unit
def test_projection_trigger_invalidates_unscoped_insert_before_marking_it():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    sql = migration.INSTALL_SQL

    assert "CREATE CONSTRAINT TRIGGER" in sql
    assert "AFTER INSERT OR UPDATE OR DELETE ON model_hub_score" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "NEW.tracer_project_id IS NULL" in sql
    assert "ready = false" in sql
    assert "WHERE singleton_id = 1 AND ready" not in sql
    assert "SELECT value_text, ref_count" in sql
    assert "FOR UPDATE" in sql
    assert sql.count("ORDER BY extracted.value_text") == 2
    assert "CREATE OR REPLACE FUNCTION model_hub_annotation_organization_lock" in sql
    assert "CREATE OR REPLACE FUNCTION model_hub_annotation_label_is_categorical" in sql
    assert "label.type = 'categorical'" in sql
    assert (
        "BEFORE UPDATE OF type, organization_id, project_id, workspace_id\n"
        "ON model_hub_annotationslabels"
    ) in sql
    assert "pg_advisory_xact_lock" in sql
    assert "model_hub_annotation_labels_lock" not in sql
    assert "futureagi.annotation_projection_organization" in sql
    assert "cross-organization Score transactions" in sql
    assert sql.index("ready = false") < sql.index("IF TG_OP = 'INSERT' THEN")
    assert "VALUES (NEW.id, NEW.organization_id, 1, new_oversize)" in sql


@pytest.mark.unit
def test_projection_lock_and_readiness_are_tenant_scoped():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    lock_function = migration.INSTALL_SQL.split(
        "CREATE OR REPLACE FUNCTION model_hub_annotation_organization_lock", 1
    )[1].split(
        "CREATE OR REPLACE FUNCTION model_hub_annotation_label_is_categorical", 1
    )[0]

    assert "current_setting('futureagi.annotation_projection_organization', true)" in (
        lock_function
    )
    assert lock_function.index("cross-organization Score transactions") < (
        lock_function.index("pg_advisory_xact_lock")
    )
    assert "input_organization_id::text" in lock_function
    assert "singleton_id" not in migration.INSTALL_SQL
    assert "ON CONFLICT (organization_id)" in migration.INSTALL_SQL

    from model_hub.models.annotation_score_values import (
        AnnotationScoreValueProjectionStatus,
    )

    assert AnnotationScoreValueProjectionStatus._meta.pk.name == "organization_id"


@pytest.mark.unit
def test_projection_scope_guard_precedes_every_new_value_projection():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    sql = migration.INSTALL_SQL
    scope_guard = sql.split(
        "CREATE OR REPLACE FUNCTION model_hub_annotation_score_scope_is_valid", 1
    )[1].split(
        "CREATE OR REPLACE FUNCTION model_hub_annotation_label_type_immutable", 1
    )[0]

    assert "project.organization_id = input_score_organization_id" in scope_guard
    assert "label.organization_id = input_score_organization_id" in scope_guard
    assert "label.project_id = input_tracer_project_id" in scope_guard
    assert "label.workspace_id = project.workspace_id" in scope_guard
    assert "FOR SHARE OF project, label" in scope_guard

    increment = sql.split(
        "CREATE OR REPLACE FUNCTION model_hub_annotation_value_increment", 1
    )[1].split("CREATE OR REPLACE FUNCTION model_hub_annotation_value_decrement", 1)[0]
    assert increment.index("model_hub_annotation_score_scope_is_valid") < (
        increment.index("INSERT INTO model_hub_annotation_value_vocab")
    )

    projector = sql.split(
        "CREATE OR REPLACE FUNCTION model_hub_project_annotation_score_value", 1
    )[1].split("CREATE OR REPLACE FUNCTION model_hub_sync_annotation_score_value", 1)[0]
    assert projector.index("model_hub_annotation_score_scope_is_valid") < (
        projector.index("INSERT INTO model_hub_annotation_value_marker")
    )
    assert projector.index("model_hub_annotation_score_scope_is_valid") < (
        projector.index("model_hub_annotation_value_increment")
    )

    trigger = sql.split(
        "CREATE OR REPLACE FUNCTION model_hub_sync_annotation_score_value", 1
    )[1].split(
        "DROP TRIGGER IF EXISTS model_hub_sync_annotation_score_value_trigger", 1
    )[0]
    assert trigger.index("model_hub_annotation_score_scope_is_valid") < trigger.index(
        "INSERT INTO model_hub_annotation_value_marker"
    )
    assert "new_live := new_candidate AND new_scope_valid" in trigger
    assert trigger.index("ready = false") < trigger.index(
        "IF NOT new_live AND (TG_OP = 'INSERT' OR NOT old_live)"
    )
    assert "IF TG_OP = 'UPDATE' AND old_live AND NOT new_live THEN" in trigger


@pytest.mark.unit
def test_projected_label_scope_cannot_drift_behind_projection():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    sql = migration.INSTALL_SQL
    label_guard = sql.split(
        "CREATE OR REPLACE FUNCTION model_hub_annotation_label_type_immutable", 1
    )[1].split("SET LOCAL lock_timeout", 1)[0]

    assert "NEW.organization_id, NEW.project_id, NEW.workspace_id" in label_guard
    assert "OLD.organization_id, OLD.project_id, OLD.workspace_id" in label_guard
    assert "model_hub_annotation_value_marker" in label_guard
    assert "annotation label projection scope cannot be changed" in label_guard


@pytest.mark.unit
def test_projection_backfill_and_gate_are_categorical_only():
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    source_text = __import__("inspect").getsource(command)

    assert source_text.count("INNER JOIN model_hub_annotationslabels AS label") == 2
    assert source_text.count("label.type = 'categorical'") == 2
    assert "s.organization_id = %s" in source_text
    assert "score.organization_id = %s" in source_text
    assert "project.organization_id = s.organization_id" in source_text
    assert "label.organization_id = s.organization_id" in source_text
    assert "label.project_id = s.tracer_project_id" in source_text
    assert "label.workspace_id = project.workspace_id" in source_text
    assert "AS integrity_mismatch" in source_text


@pytest.mark.unit
def test_projection_rollout_is_atomic_bounded_and_has_no_migration_tenant_scan():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    sql = migration.INSTALL_SQL
    status_helper = sql.split(
        "CREATE OR REPLACE FUNCTION model_hub_annotation_status_ensure_exists", 1
    )[1].split(
        "CREATE OR REPLACE FUNCTION model_hub_project_annotation_score_value", 1
    )[0]
    terminal_sql = sql.split(
        "FOR EACH ROW EXECUTE FUNCTION model_hub_sync_annotation_score_value();", 1
    )[1]

    assert ") VALUES (input_organization_id, 1, false, 0, NULL, NOW())" in (
        status_helper
    )
    assert "input_organization_id, 1, true" not in status_helper
    assert "SELECT DISTINCT organization_id" not in terminal_sql
    assert "FROM tracer_project" not in terminal_sql
    assert "FROM model_hub_score" not in terminal_sql
    assert "\nEND\n$function$;" not in sql
    assert migration.Migration.atomic is True

    label_ddl = sql.index(
        "DROP TRIGGER IF EXISTS model_hub_annotation_label_type_immutable_trigger"
    )
    score_ddl = sql.index(
        "DROP TRIGGER IF EXISTS model_hub_sync_annotation_score_value_trigger"
    )
    lock_timeouts = [
        index
        for index in range(len(sql))
        if sql.startswith("SET LOCAL lock_timeout = '5s';", index)
    ]
    statement_timeouts = [
        index
        for index in range(len(sql))
        if sql.startswith("SET LOCAL statement_timeout = '30s';", index)
    ]
    assert len(lock_timeouts) == 2
    assert len(statement_timeouts) == 2
    assert lock_timeouts[0] < statement_timeouts[0] < label_ddl
    assert lock_timeouts[1] < statement_timeouts[1] < score_ddl
    assert migration.UNINSTALL_SQL.count("SET LOCAL lock_timeout = '5s';") == 2
    assert migration.UNINSTALL_SQL.count("SET LOCAL statement_timeout = '30s';") == 2


@pytest.mark.unit
def test_backfill_command_owns_status_discovery_and_fail_closed_creation():
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    source_text = __import__("inspect").getsource(command)

    assert (
        "SELECT id AS organization_id\n                FROM accounts_organization"
        in (source_text)
    )
    assert "FROM model_hub_annotation_value_status" in source_text
    assert "status.updated_at NULLS FIRST" in source_text
    assert "LIMIT %s" in source_text
    assert ") VALUES (%s, %s, false, 0, NULL, NOW())" in source_text
    assert "_ensure_status(organization_id)" in source_text


@pytest.mark.unit
def test_projector_rechecks_live_scope_after_lock_before_inserting_marker():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    projector = migration.INSTALL_SQL.split(
        "CREATE OR REPLACE FUNCTION model_hub_project_annotation_score_value", 1
    )[1].split("CREATE OR REPLACE FUNCTION model_hub_sync_annotation_score_value", 1)[0]

    live_scope_guard = projector.index("IF score_row.deleted")
    marker_insert = projector.index("INSERT INTO model_hub_annotation_value_marker")
    assert live_scope_guard < marker_insert
    assert "FOR UPDATE SKIP LOCKED" in projector
    assert projector.index("model_hub_annotation_organization_lock") < marker_insert
    assert "score_row.tracer_project_id IS NULL" in projector
    assert (
        "score_row.trace_id IS NULL AND score_row.observation_span_id IS NULL"
        in projector
    )


@pytest.mark.unit
def test_annotation_label_type_change_is_rejected_without_database_access():
    from rest_framework import serializers

    from model_hub.serializers.develop_annotations import AnnotationsLabelsSerializer

    serializer = AnnotationsLabelsSerializer()
    serializer.instance = SimpleNamespace(
        type="text",
        name="immutable",
        project=None,
    )

    with pytest.raises(serializers.ValidationError, match="cannot be changed"):
        serializer.validate({"type": "categorical"})

    assert serializer.validate({"type": "text"})["type"] == "text"


@pytest.mark.unit
def test_projection_trigger_invalidates_scoped_to_unscoped_before_decrement():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    sql = migration.INSTALL_SQL

    invalidation = sql.index("ready = false")
    old_scope_decrement = sql.index(
        "OLD.tracer_project_id, OLD.label_id, OLD.value",
        invalidation,
    )
    assert invalidation < old_scope_decrement
    assert "old_live AND NOT old_oversize" in sql


@pytest.mark.unit
def test_projection_delete_uses_marker_as_categorical_membership_proof():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    delete_slice = migration.INSTALL_SQL.split("IF TG_OP = 'DELETE' THEN", 1)[1].split(
        "RETURN OLD;", 1
    )[0]

    assert "DELETE FROM model_hub_annotation_value_marker" in delete_slice
    assert "model_hub_annotation_label_is_categorical" not in delete_slice
    assert "SELECT organization_id INTO marker_organization_id" in delete_slice
    assert (
        "model_hub_annotation_organization_lock(marker_organization_id)" in delete_slice
    )


@pytest.mark.unit
def test_projection_trigger_blocks_oversize_before_vocab_and_reader_fails_closed(
    monkeypatch,
):
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    sql = migration.INSTALL_SQL

    assert "blocked_oversize = new_oversize" in sql
    assert "model_hub_annotation_payload_has_oversize(NEW.value)" in sql
    assert "octet_length(extracted.value_text) > 16384" in sql
    assert "octet_length(extracted.value_text) <= 16384" in sql
    assert "octet_length(payload::text) > 65536" in sql
    assert "SELECT count(*) > 256" in sql
    assert "AND NOT new_oversize" in sql
    assert "IF NOT blocked_oversize THEN" in sql
    assert "octet_length(value_search) <= 16384" in sql

    cursor = _Cursor([(False, 1, 23, False, None, None, None)])
    monkeypatch.setattr(
        source,
        "connection",
        SimpleNamespace(cursor=lambda: cursor),
    )
    with pytest.raises(source.AnnotationScoreReadUnavailable, match="not ready"):
        source.AnnotationLabelScoresProjectPG().categorical_value_page_for_label(
            str(uuid4()),
            [str(uuid4())],
            page_size=10,
        )


@pytest.mark.unit
def test_projection_byte_limit_is_explicit_and_multibyte_safe():
    migration = importlib.import_module(
        "model_hub.migrations.0123_annotation_score_value_projection"
    )
    at_limit = "🙂" * (source.ANNOTATION_SUGGESTION_VALUE_MAX_BYTES // 4)
    over_limit = at_limit + "🙂"

    assert len(at_limit.encode("utf-8")) == 16 * 1024
    assert len(over_limit.encode("utf-8")) > 16 * 1024
    assert migration.SUGGESTION_VALUE_MAX_BYTES == (
        source.ANNOTATION_SUGGESTION_VALUE_MAX_BYTES
    )
    assert migration.SUGGESTION_PAYLOAD_MAX_BYTES == (
        source.ANNOTATION_SUGGESTION_PAYLOAD_MAX_BYTES
    )
    assert migration.SUGGESTION_VALUES_PER_SCORE_MAX == (
        source.ANNOTATION_SUGGESTION_VALUES_PER_SCORE_MAX
    )


@pytest.mark.unit
def test_projection_gate_counts_oversize_blockers(monkeypatch):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    organization_id = str(uuid4())
    monkeypatch.setattr(command, "_organization_ids", lambda _limit: [organization_id])
    monkeypatch.setattr(
        command,
        "_projection_gate",
        lambda requested_organization_id, **_kwargs: {
            "ready": False,
            "pending": 0,
            "unscoped": 0,
            "oversize": 1,
            "integrity_mismatch": 0,
            "projected_scores": 0,
        },
    )

    result = command.backfill_annotation_score_values(audit_only=True)

    assert result["ready"] is False
    assert result["oversize"] == 1


@pytest.mark.unit
def test_projection_publish_gate_locks_status_before_coverage_snapshot(monkeypatch):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )

    class GateCursor:
        rowcount = 1

        def __init__(self):
            self.queries = []
            self.rows = iter([(1,), (0, 0, 0, 0), (19,)])

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params=None):
            self.queries.append((" ".join(query.split()), params))

        def fetchone(self):
            return next(self.rows)

    cursor = GateCursor()
    monkeypatch.setattr(
        command,
        "connection",
        SimpleNamespace(cursor=lambda: cursor),
    )
    monkeypatch.setattr(
        command.transaction,
        "atomic",
        nullcontext,
    )

    organization_id = str(uuid4())
    result = command._projection_gate(organization_id, publish=True)

    status_read = next(
        query
        for query, _params in cursor.queries
        if "SELECT projection_version" in query
    )
    coverage_read = next(
        query for query, _params in cursor.queries if "AS unscoped" in query
    )
    assert "FOR UPDATE" in status_read
    organization_lock = next(
        query
        for query, _params in cursor.queries
        if "model_hub_annotation_organization_lock" in query
    )
    assert cursor.queries.index(
        (organization_lock, [organization_id])
    ) < cursor.queries.index((status_read, [organization_id]))
    assert cursor.queries.index((status_read, [organization_id])) < next(
        index
        for index, (query, _params) in enumerate(cursor.queries)
        if query == coverage_read
    )
    assert result == {
        "ready": True,
        "pending": 0,
        "unscoped": 0,
        "oversize": 0,
        "integrity_mismatch": 0,
        "projected_scores": 19,
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    ("status_row", "coverage", "expected_mismatch"),
    [
        (None, (0, 0, 0, 0), 0),
        ((1,), (0, 0, 0, 1), 1),
    ],
)
def test_projection_gate_fails_closed_for_missing_status_or_scope_mismatch(
    monkeypatch,
    status_row,
    coverage,
    expected_mismatch,
):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )

    class GateCursor:
        def __init__(self):
            self.rows = iter((status_row, coverage))

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _query, _params=None):
            return None

        def fetchone(self):
            return next(self.rows)

    monkeypatch.setattr(
        command,
        "connection",
        SimpleNamespace(cursor=GateCursor),
    )
    monkeypatch.setattr(command.transaction, "atomic", nullcontext)

    result = command._projection_gate(str(uuid4()), publish=False)

    assert result["ready"] is False
    assert result["integrity_mismatch"] == expected_mismatch
    assert result["projected_scores"] == 0


@pytest.mark.unit
def test_projection_backfill_resumes_by_keyset_and_wraps_once(monkeypatch):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    organization_id = str(uuid4())
    first_id = str(uuid4())
    second_id = str(uuid4())
    observed_after = []
    pending = iter([[first_id, second_id], [], []])
    reset_calls = []

    monkeypatch.setattr(command, "_organization_ids", lambda _limit: [organization_id])
    monkeypatch.setattr(command, "_ensure_status", lambda _organization_id: None)
    monkeypatch.setattr(command, "_backfill_cursor", lambda _organization_id: first_id)

    def pending_ids(*, organization_id: str, batch_size, after_id):
        assert organization_id
        assert batch_size == command.DEFAULT_BATCH_SIZE
        observed_after.append(after_id)
        return next(pending)

    monkeypatch.setattr(command, "_pending_score_ids", pending_ids)
    monkeypatch.setattr(
        command, "_project_score_ids", lambda _organization_id, ids: len(ids)
    )
    monkeypatch.setattr(
        command,
        "_reset_backfill_cursor",
        lambda _organization_id: reset_calls.append(True),
    )
    published = []

    def gate(requested_organization_id, *, publish):
        assert requested_organization_id == organization_id
        published.append(publish)
        return {
            "ready": True,
            "pending": 0,
            "unscoped": 0,
            "oversize": 0,
            "integrity_mismatch": 0,
            "projected_scores": 2,
        }

    monkeypatch.setattr(command, "_projection_gate", gate)

    result = command.backfill_annotation_score_values()

    assert result["projected_this_run"] == 2
    assert observed_after == [first_id, second_id, None]
    assert reset_calls == [True]
    assert published == [True]


def _ready_projection_gate(projected_scores=0):
    return {
        "ready": True,
        "pending": 0,
        "unscoped": 0,
        "oversize": 0,
        "integrity_mismatch": 0,
        "projected_scores": projected_scores,
    }


@pytest.mark.unit
def test_projection_discovers_zero_score_tenants_with_a_fair_bounded_page(
    monkeypatch,
):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    organization_id = str(uuid4())
    cursor = _Cursor([(organization_id,)])
    monkeypatch.setattr(command, "_bounded_cursor", lambda: nullcontext(cursor))
    monkeypatch.setattr(command, "connection", SimpleNamespace(vendor=None))

    assert command._organization_ids(8) == [organization_id]

    compact_sql = " ".join(cursor.query.split())
    assert "FROM accounts_organization" in compact_sql
    assert "UNION SELECT organization_id" in compact_sql
    assert "FROM model_hub_annotation_value_status" in compact_sql
    assert "ORDER BY (status.organization_id IS NOT NULL)" in compact_sql
    assert "status.updated_at NULLS FIRST" in compact_sql
    assert "LIMIT %s" in compact_sql
    assert cursor.params == [8]


@pytest.mark.unit
def test_scheduled_projection_only_retries_due_or_stale_tenants(monkeypatch):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    cursor = _Cursor([])
    monkeypatch.setattr(command, "_bounded_cursor", lambda: nullcontext(cursor))
    monkeypatch.setattr(command, "connection", SimpleNamespace(vendor=None))

    command._organization_ids(
        101,
        retry_after_seconds=60,
        ready_recheck_after_seconds=86_400,
    )

    compact_sql = " ".join(cursor.query.split())
    assert "status.organization_id IS NULL" in compact_sql
    assert "WHEN status.ready THEN %s ELSE %s" in compact_sql
    assert "status.updated_at <= NOW() - make_interval" in compact_sql
    assert cursor.params == [86_400, 60, 101]


@pytest.mark.unit
def test_zero_score_new_tenant_is_published_ready_by_reconcile(monkeypatch):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    organization_id = str(uuid4())
    events = []

    monkeypatch.setattr(command, "_organization_ids", lambda _limit: [organization_id])
    monkeypatch.setattr(
        command,
        "_ensure_status",
        lambda requested: events.append(("ensure", requested)),
    )
    monkeypatch.setattr(command, "_backfill_cursor", lambda _requested: None)
    monkeypatch.setattr(command, "_pending_score_ids", lambda **_kwargs: [])

    def gate(requested, *, publish):
        events.append(("gate", requested, publish))
        return _ready_projection_gate()

    monkeypatch.setattr(command, "_projection_gate", gate)

    result = command.backfill_annotation_score_values(max_organizations=1)

    assert events == [
        ("ensure", organization_id),
        ("gate", organization_id, True),
    ]
    assert result["ready"] is True
    assert result["projected_this_run"] == 0
    assert result["selected_this_run"] == 0


@pytest.mark.unit
def test_projection_score_cap_counts_selected_rows_even_when_claim_is_raced(
    monkeypatch,
):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    organization_id = str(uuid4())
    score_ids = [str(uuid4()), str(uuid4())]
    pending_calls = []
    publishes = []

    monkeypatch.setattr(command, "_organization_ids", lambda _limit: [organization_id])
    monkeypatch.setattr(command, "_ensure_status", lambda _requested: None)
    monkeypatch.setattr(command, "_backfill_cursor", lambda _requested: None)

    def pending(**kwargs):
        pending_calls.append(kwargs)
        return score_ids

    monkeypatch.setattr(command, "_pending_score_ids", pending)
    monkeypatch.setattr(command, "_project_score_ids", lambda *_args: 0)

    def gate(_requested, *, publish):
        publishes.append(publish)
        return {**_ready_projection_gate(), "ready": False, "pending": 2}

    monkeypatch.setattr(command, "_projection_gate", gate)

    result = command.backfill_annotation_score_values(
        batch_size=10,
        max_scores=2,
        max_organizations=1,
    )

    assert len(pending_calls) == 1
    assert pending_calls[0]["batch_size"] == 2
    assert publishes == [False]
    assert result["selected_this_run"] == 2
    assert result["projected_this_run"] == 0
    assert result["score_limit_exhausted"] is True
    assert result["ready"] is False


@pytest.mark.unit
def test_projection_tenant_page_sentinel_prevents_false_global_readiness(monkeypatch):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    first_id = str(uuid4())
    second_id = str(uuid4())

    monkeypatch.setattr(
        command, "_organization_ids", lambda _limit: [first_id, second_id]
    )
    monkeypatch.setattr(command, "_ensure_status", lambda _requested: None)
    monkeypatch.setattr(command, "_backfill_cursor", lambda _requested: None)
    monkeypatch.setattr(command, "_pending_score_ids", lambda **_kwargs: [])
    monkeypatch.setattr(
        command,
        "_projection_gate",
        lambda _requested, **_kwargs: _ready_projection_gate(),
    )

    result = command.backfill_annotation_score_values(max_organizations=1)

    assert result["organizations"] == 1
    assert result["organizations_processed"] == 1
    assert result["organization_page_has_more"] is True
    assert result["ready"] is False


@pytest.mark.unit
def test_projection_continue_on_error_rotates_tenant_and_keeps_progress(
    monkeypatch,
):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )
    failed_id = str(uuid4())
    healthy_id = str(uuid4())
    deferred = []
    gated = []

    monkeypatch.setattr(
        command, "_organization_ids", lambda _limit: [failed_id, healthy_id]
    )

    def ensure(requested):
        if requested == failed_id:
            raise RuntimeError("busy tenant")

    monkeypatch.setattr(command, "_ensure_status", ensure)
    monkeypatch.setattr(command, "_defer_status", deferred.append)
    monkeypatch.setattr(command, "_backfill_cursor", lambda _requested: None)
    monkeypatch.setattr(command, "_pending_score_ids", lambda **_kwargs: [])

    def gate(requested, **_kwargs):
        gated.append(requested)
        return _ready_projection_gate()

    monkeypatch.setattr(command, "_projection_gate", gate)

    result = command.backfill_annotation_score_values(
        max_organizations=2,
        continue_on_error=True,
    )

    assert deferred == [failed_id]
    assert gated == [healthy_id]
    assert result["errors"] == 1
    assert result["organizations_processed"] == 2
    assert result["ready"] is False


@pytest.mark.unit
def test_projection_statements_use_shrinking_transaction_local_timeouts(monkeypatch):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )

    class RecordingCursor:
        def __init__(self):
            self.queries = []

        def execute(self, query, params=None):
            self.queries.append((" ".join(query.split()), params))

    cursor = RecordingCursor()
    now = iter((2.0, 8.25))
    monkeypatch.setattr(command.time, "monotonic", lambda: next(now))
    monkeypatch.setattr(command, "connection", SimpleNamespace(vendor="postgresql"))
    token = command._ACTIVE_BUDGET.set(
        command._ProjectionBudget(
            deadline=10.0,
            statement_timeout_ms=5_000,
            lock_timeout_ms=1_000,
        )
    )
    try:
        command._execute(cursor, "SELECT one")
        command._execute(cursor, "SELECT two")
    finally:
        command._ACTIVE_BUDGET.reset(token)

    timeout_calls = [
        params for query, params in cursor.queries if "set_config" in query
    ]
    assert timeout_calls == [
        ["1000ms", "5000ms"],
        ["1000ms", "1750ms"],
    ]
    assert [
        query for query, _params in cursor.queries if "set_config" not in query
    ] == [
        "SELECT one",
        "SELECT two",
    ]


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_scores": 0},
        {"max_scores": 1_000_001},
        {"max_organizations": 0},
        {"max_organizations": 1_001},
        {"max_runtime_seconds": 0},
        {"max_runtime_seconds": 3_601},
        {"max_runtime_seconds": float("nan")},
        {"sleep_seconds": float("nan")},
        {"statement_timeout_ms": 30_001},
        {"lock_timeout_ms": 5_001},
        {"statement_timeout_ms": 100, "lock_timeout_ms": 101},
        {"retry_after_seconds": 60},
        {
            "retry_after_seconds": 120,
            "ready_recheck_after_seconds": 60,
        },
        {
            "retry_after_seconds": 60,
            "ready_recheck_after_seconds": 31 * 24 * 60 * 60 + 1,
        },
    ],
)
def test_projection_rejects_unbounded_or_invalid_run_limits(kwargs):
    command = importlib.import_module(
        "model_hub.management.commands.backfill_annotation_score_values"
    )

    with pytest.raises(ValueError):
        command.backfill_annotation_score_values(**kwargs)


@pytest.mark.unit
def test_projection_repair_is_registered_as_a_finite_recurring_activity(monkeypatch):
    from model_hub.tasks import annotation_score_values as task
    from tfc.temporal.common.registry import TEMPORAL_ACTIVITY_MODULES
    from tfc.temporal.schedules.model_hub import MODEL_HUB_SCHEDULES

    captured = {}

    def reconcile(**kwargs):
        captured.update(kwargs)
        return {"ready": True}

    monkeypatch.setattr(task, "backfill_annotation_score_values", reconcile)

    assert task.reconcile_annotation_score_values._original_func() == {"ready": True}
    assert captured == {
        "batch_size": 100,
        "max_scores": 1_000,
        "max_organizations": 100,
        "max_runtime_seconds": 45.0,
        "statement_timeout_ms": 5_000,
        "lock_timeout_ms": 1_000,
        "retry_after_seconds": 60,
        "ready_recheck_after_seconds": 86_400,
        "continue_on_error": True,
    }
    assert task.reconcile_annotation_score_values._metadata["time_limit"] == 55
    assert task.reconcile_annotation_score_values._metadata["max_retries"] == 1
    assert "model_hub.tasks.annotation_score_values" in TEMPORAL_ACTIVITY_MODULES

    schedule = next(
        item
        for item in MODEL_HUB_SCHEDULES
        if item.schedule_id == "reconcile-annotation-score-values"
    )
    assert schedule.activity_name == "reconcile_annotation_score_values"
    assert schedule.interval_seconds == 60
    assert schedule.catchup_window_seconds == 300
    assert schedule.queue == "default"


@pytest.mark.unit
def test_projection_operator_command_remains_explicitly_allowlisted():
    from model_hub.apps import OPERATOR_STARTUP_MUTATION_COMMANDS

    assert "backfill_annotation_score_values" in OPERATOR_STARTUP_MUTATION_COMMANDS
