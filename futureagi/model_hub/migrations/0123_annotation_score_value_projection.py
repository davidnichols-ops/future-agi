"""Add an exact, trigger-maintained vocabulary projection for Score values.

The tables are empty at migration time.  The trigger immediately protects new
writes, while ``backfill_annotation_score_values`` claims historic Score rows
idempotently.  The public reader stays fail-closed until that command proves no
eligible unclaimed row remains for every requested organization and publishes
that organization's readiness row.  Projection health is never global: one
tenant's malformed Score cannot make another tenant's picker unavailable.
"""

import django.contrib.postgres.indexes
from django.db import migrations, models

PROJECTION_VERSION = 1
SUGGESTION_VALUE_MAX_BYTES = 16 * 1024
SUGGESTION_PAYLOAD_MAX_BYTES = 64 * 1024
SUGGESTION_VALUES_PER_SCORE_MAX = 256


INSTALL_SQL = r"""
ALTER TABLE model_hub_annotation_value_vocab
ADD CONSTRAINT annv_value_max_bytes
CHECK (
    octet_length(value_text) <= 16384
    AND octet_length(value_search) <= 16384
);

CREATE OR REPLACE FUNCTION model_hub_annotation_value_to_text(input_value jsonb)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $function$
    SELECT CASE jsonb_typeof(input_value)
        WHEN 'string' THEN input_value #>> '{}'
        WHEN 'boolean' THEN CASE input_value::text
            WHEN 'true' THEN 'True'
            ELSE 'False'
        END
        WHEN 'number' THEN input_value #>> '{}'
        WHEN 'null' THEN NULL
        ELSE input_value::text
    END
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_score_value_texts(payload jsonb)
RETURNS TABLE(value_text text)
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $function$
DECLARE
    normalized jsonb := payload;
    decoded jsonb;
    candidate_json jsonb;
    candidate_text text;
    key_name text;
BEGIN
    IF normalized IS NULL OR jsonb_typeof(normalized) = 'null' THEN
        RETURN;
    END IF;

    -- Historic imports can contain one additional JSON encoding layer.  This
    -- mirrors the former Python ``json.loads``-when-possible behavior.
    IF jsonb_typeof(normalized) = 'string' THEN
        BEGIN
            decoded := (normalized #>> '{}')::jsonb;
            normalized := decoded;
        EXCEPTION WHEN invalid_text_representation THEN
            NULL;
        END;
    END IF;

    IF jsonb_typeof(normalized) = 'object' THEN
        IF normalized ? 'selected' THEN
            candidate_json := normalized -> 'selected';
            IF jsonb_typeof(candidate_json) = 'array' THEN
                FOR candidate_json IN
                    SELECT items.element
                    FROM jsonb_array_elements(candidate_json) AS items(element)
                LOOP
                    candidate_text := model_hub_annotation_value_to_text(candidate_json);
                    IF candidate_text IS NOT NULL AND candidate_text <> '' THEN
                        value_text := candidate_text;
                        RETURN NEXT;
                    END IF;
                END LOOP;
            ELSE
                candidate_text := model_hub_annotation_value_to_text(candidate_json);
                IF candidate_text IS NOT NULL AND candidate_text <> '' THEN
                    value_text := candidate_text;
                    RETURN NEXT;
                END IF;
            END IF;
        END IF;

        FOREACH key_name IN ARRAY ARRAY['value', 'label', 'text'] LOOP
            IF normalized ? key_name THEN
                candidate_text := model_hub_annotation_value_to_text(
                    normalized -> key_name
                );
                IF candidate_text IS NOT NULL AND candidate_text <> '' THEN
                    value_text := candidate_text;
                    RETURN NEXT;
                END IF;
            END IF;
        END LOOP;
        RETURN;
    END IF;

    IF jsonb_typeof(normalized) = 'array' THEN
        FOR candidate_json IN
            SELECT items.element
            FROM jsonb_array_elements(normalized) AS items(element)
        LOOP
            candidate_text := model_hub_annotation_value_to_text(candidate_json);
            IF candidate_text IS NOT NULL AND candidate_text <> '' THEN
                value_text := candidate_text;
                RETURN NEXT;
            END IF;
        END LOOP;
        RETURN;
    END IF;

    candidate_text := model_hub_annotation_value_to_text(normalized);
    IF candidate_text IS NOT NULL AND candidate_text <> '' THEN
        value_text := candidate_text;
        RETURN NEXT;
    END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_payload_has_oversize(payload jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $function$
    SELECT CASE
        WHEN payload IS NULL THEN false
        -- Bound the work before expanding a pathological multi-MB list or
        -- wrapper. A blocked Score contributes no vocabulary rows and keeps
        -- the public reader closed until it is repaired or deleted.
        WHEN octet_length(payload::text) > 65536 THEN true
        ELSE
            EXISTS (
                SELECT 1
                FROM model_hub_annotation_score_value_texts(payload) AS extracted
                WHERE octet_length(extracted.value_text) > 16384
                   OR octet_length(lower(extracted.value_text)) > 16384
            )
            OR (
                SELECT count(*) > 256
                FROM (
                    SELECT 1
                    FROM model_hub_annotation_score_value_texts(payload)
                    LIMIT 257
                ) AS bounded_values
            )
    END
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_digest_guard(
    existing_value text,
    incoming_value text,
    next_count bigint
)
RETURNS bigint
LANGUAGE plpgsql
IMMUTABLE
AS $function$
BEGIN
    IF existing_value IS DISTINCT FROM incoming_value THEN
        RAISE EXCEPTION 'annotation value digest collision'
            USING ERRCODE = 'unique_violation';
    END IF;
    RETURN next_count;
END;
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_organization_lock(
    input_organization_id uuid
)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    transaction_organization text;
BEGIN
    IF input_organization_id IS NULL THEN
        RAISE EXCEPTION 'annotation projection requires an organization'
            USING ERRCODE = 'not_null_violation';
    END IF;

    -- A transaction may touch any number of Scores/labels/projects inside one
    -- organization, but never two organizations.  Recording that invariant in
    -- transaction-local state means the transaction can acquire exactly one
    -- projection lock.  Unlike per-label locks, later statements cannot obtain
    -- the same lock set in a different order and form a cycle.
    transaction_organization := NULLIF(
        current_setting('futureagi.annotation_projection_organization', true),
        ''
    );
    IF transaction_organization IS NULL THEN
        PERFORM set_config(
            'futureagi.annotation_projection_organization',
            input_organization_id::text,
            true
        );
    ELSIF transaction_organization <> input_organization_id::text THEN
        RAISE EXCEPTION
            'annotation projection rejects cross-organization Score transactions'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    -- Serialize only this tenant's shared vocabulary/ref-count/revision rows.
    -- The 64-bit hash collision risk is conservative serialization only; it
    -- cannot mix readiness or data because every table query remains scoped by
    -- the original UUID.
    PERFORM pg_advisory_xact_lock(
        hashtextextended(input_organization_id::text, 7247)
    );
END;
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_label_is_categorical(
    input_label_id uuid
)
RETURNS boolean
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $function$
    SELECT EXISTS (
        SELECT 1
        FROM model_hub_annotationslabels AS label
        WHERE label.id = input_label_id
          AND label.type = 'categorical'
    )
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_score_scope_is_valid(
    input_score_organization_id uuid,
    input_tracer_project_id uuid,
    input_label_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
AS $function$
DECLARE
    scope_valid boolean := false;
BEGIN
    -- A project-local label must name this exact tracer project. A
    -- workspace-local label may be used only by a project in that workspace.
    -- Organization-global labels keep both nullable scope columns empty. Row
    -- locks close the validation/write race until this transaction commits.
    SELECT true INTO scope_valid
    FROM tracer_project AS project
    INNER JOIN model_hub_annotationslabels AS label
      ON label.id = input_label_id
    WHERE project.id = input_tracer_project_id
      AND project.organization_id = input_score_organization_id
      AND label.organization_id = input_score_organization_id
      AND (
          label.project_id IS NULL
          OR label.project_id = input_tracer_project_id
      )
      AND (
          label.workspace_id IS NULL
          OR label.workspace_id = project.workspace_id
      )
    FOR SHARE OF project, label;
    RETURN COALESCE(scope_valid, false);
END;
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_label_type_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF NEW.type IS DISTINCT FROM OLD.type THEN
        RAISE EXCEPTION 'annotation label type cannot be changed'
            USING ERRCODE = 'check_violation';
    END IF;
    IF ROW(NEW.organization_id, NEW.project_id, NEW.workspace_id)
       IS DISTINCT FROM
       ROW(OLD.organization_id, OLD.project_id, OLD.workspace_id)
       AND EXISTS (
           SELECT 1
           FROM model_hub_score AS score
           INNER JOIN model_hub_annotation_value_marker AS marker
             ON marker.score_id = score.id
            AND marker.projection_version = 1
           WHERE score.label_id = OLD.id
           LIMIT 1
       )
    THEN
        RAISE EXCEPTION 'annotation label projection scope cannot be changed'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$function$;

-- These tables are hot in hosted deployments. Keep this migration atomic, but
-- fail the deploy promptly if trigger DDL cannot acquire its lock.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DROP TRIGGER IF EXISTS model_hub_annotation_label_type_immutable_trigger
ON model_hub_annotationslabels;
CREATE TRIGGER model_hub_annotation_label_type_immutable_trigger
BEFORE UPDATE OF type, organization_id, project_id, workspace_id
ON model_hub_annotationslabels
FOR EACH ROW EXECUTE FUNCTION model_hub_annotation_label_type_immutable();

CREATE OR REPLACE FUNCTION model_hub_annotation_value_increment(
    input_organization_id uuid,
    input_project_id uuid,
    input_label_id uuid,
    input_payload jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    candidate_text text;
    candidate_digest bytea;
BEGIN
    IF NOT model_hub_annotation_score_scope_is_valid(
        input_organization_id,
        input_project_id,
        input_label_id
    ) THEN
        RAISE EXCEPTION 'annotation value projection scope is invalid'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;

    FOR candidate_text IN
        SELECT DISTINCT extracted.value_text
        FROM model_hub_annotation_score_value_texts(input_payload) AS extracted
        WHERE extracted.value_text IS NOT NULL AND extracted.value_text <> ''
          AND octet_length(extracted.value_text) <= 16384
          AND octet_length(lower(extracted.value_text)) <= 16384
        ORDER BY extracted.value_text
    LOOP
        candidate_digest := digest(convert_to(candidate_text, 'UTF8'), 'sha256');
        INSERT INTO model_hub_annotation_value_vocab (
            tracer_project_id,
            label_id,
            value_digest,
            value_text,
            value_sort_prefix,
            value_search,
            ref_count
        ) VALUES (
            input_project_id,
            input_label_id,
            candidate_digest,
            candidate_text,
            left(lower(candidate_text), 384),
            lower(candidate_text),
            1
        )
        ON CONFLICT (tracer_project_id, label_id, value_digest)
        DO UPDATE SET
            ref_count = model_hub_annotation_digest_guard(
                model_hub_annotation_value_vocab.value_text,
                EXCLUDED.value_text,
                model_hub_annotation_value_vocab.ref_count + 1
            );
    END LOOP;
END;
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_value_decrement(
    input_project_id uuid,
    input_label_id uuid,
    input_payload jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
    candidate_text text;
    candidate_digest bytea;
    existing_value text;
    existing_ref_count bigint;
BEGIN
    FOR candidate_text IN
        SELECT DISTINCT extracted.value_text
        FROM model_hub_annotation_score_value_texts(input_payload) AS extracted
        WHERE extracted.value_text IS NOT NULL AND extracted.value_text <> ''
          AND octet_length(extracted.value_text) <= 16384
          AND octet_length(lower(extracted.value_text)) <= 16384
        ORDER BY extracted.value_text
    LOOP
        candidate_digest := digest(convert_to(candidate_text, 'UTF8'), 'sha256');

        SELECT value_text, ref_count
        INTO existing_value, existing_ref_count
        FROM model_hub_annotation_value_vocab
        WHERE tracer_project_id = input_project_id
          AND label_id = input_label_id
          AND value_digest = candidate_digest
        FOR UPDATE;
        IF NOT FOUND OR existing_value IS DISTINCT FROM candidate_text THEN
            RAISE EXCEPTION 'annotation value projection is inconsistent'
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;

        IF existing_ref_count = 1 THEN
            DELETE FROM model_hub_annotation_value_vocab
            WHERE tracer_project_id = input_project_id
              AND label_id = input_label_id
              AND value_digest = candidate_digest;
        ELSE
            UPDATE model_hub_annotation_value_vocab
            SET ref_count = existing_ref_count - 1
            WHERE tracer_project_id = input_project_id
              AND label_id = input_label_id
              AND value_digest = candidate_digest;
        END IF;
    END LOOP;
END;
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_label_revision_bump(
    input_label_id uuid
)
RETURNS void
LANGUAGE sql
AS $function$
    INSERT INTO model_hub_annotation_value_label_state (label_id, revision)
    VALUES (input_label_id, 1)
    ON CONFLICT (label_id)
    DO UPDATE SET revision = model_hub_annotation_value_label_state.revision + 1
$function$;

CREATE OR REPLACE FUNCTION model_hub_annotation_status_ensure_exists(
    input_organization_id uuid
)
RETURNS void
LANGUAGE sql
AS $function$
    -- A missing row is fail-closed. Only the explicit backfill command may
    -- publish ready=true after proving complete, tenant-scoped coverage.
    INSERT INTO model_hub_annotation_value_status (
        organization_id, projection_version, ready,
        projected_scores, backfill_cursor, updated_at
    ) VALUES (input_organization_id, 1, false, 0, NULL, NOW())
    ON CONFLICT (organization_id) DO NOTHING
$function$;

CREATE OR REPLACE FUNCTION model_hub_project_annotation_score_value(
    input_score_id uuid
)
RETURNS boolean
LANGUAGE plpgsql
AS $function$
DECLARE
    score_row model_hub_score%ROWTYPE;
    claimed boolean := false;
    blocked_oversize boolean := false;
BEGIN
    SELECT * INTO score_row
    FROM model_hub_score
    WHERE id = input_score_id
    FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    -- Only categorical annotation values are exposed by this projection.
    -- Text/numeric/star/thumb Scores must never grow its GIN vocabulary or
    -- become global oversize blockers.
    IF NOT model_hub_annotation_label_is_categorical(score_row.label_id) THEN
        RETURN false;
    END IF;

    -- The pending-id scan and this row lock are intentionally separate,
    -- resumable transactions.  Re-check live projection membership after the
    -- lock so a concurrent soft delete/project clear/source clear cannot leave
    -- a marker for a row that contributed no vocabulary.  Such a marker would
    -- make a later restore fail as an inconsistent "newly scoped" Score.
    IF score_row.deleted
       OR score_row.tracer_project_id IS NULL
       OR (score_row.trace_id IS NULL AND score_row.observation_span_id IS NULL)
    THEN
        RETURN false;
    END IF;

    PERFORM model_hub_annotation_organization_lock(score_row.organization_id);

    -- Fail closed before claiming a marker or touching vocabulary. Historic
    -- cross-tenant or incompatible label scopes are audited by the backfill
    -- gate; they are never projected under the Score's tracer project.
    IF NOT model_hub_annotation_score_scope_is_valid(
        score_row.organization_id,
        score_row.tracer_project_id,
        score_row.label_id
    ) THEN
        INSERT INTO model_hub_annotation_value_status (
            organization_id, projection_version, ready,
            projected_scores, backfill_cursor, updated_at
        ) VALUES (score_row.organization_id, 1, false, 0, NULL, NOW())
        ON CONFLICT (organization_id) DO UPDATE SET
            ready = false,
            updated_at = NOW();
        RETURN false;
    END IF;

    blocked_oversize := model_hub_annotation_payload_has_oversize(
        score_row.value
    );
    INSERT INTO model_hub_annotation_value_marker (
        score_id, organization_id, projection_version, blocked_oversize
    )
    VALUES (score_row.id, score_row.organization_id, 1, blocked_oversize)
    ON CONFLICT (score_id) DO NOTHING
    RETURNING true INTO claimed;
    IF NOT COALESCE(claimed, false) THEN
        RETURN false;
    END IF;

    IF blocked_oversize THEN
        INSERT INTO model_hub_annotation_value_status (
            organization_id, projection_version, ready,
            projected_scores, backfill_cursor, updated_at
        ) VALUES (score_row.organization_id, 1, false, 0, NULL, NOW())
        ON CONFLICT (organization_id) DO UPDATE SET
            ready = false,
            updated_at = NOW();
    END IF;

    IF NOT blocked_oversize THEN
        PERFORM model_hub_annotation_value_increment(
            score_row.organization_id,
            score_row.tracer_project_id,
            score_row.label_id,
            score_row.value
        );
        PERFORM model_hub_annotation_label_revision_bump(score_row.label_id);
    END IF;
    RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION model_hub_sync_annotation_score_value()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    claimed boolean := false;
    old_relevant boolean := false;
    new_relevant boolean := false;
    new_candidate boolean := false;
    new_scope_valid boolean := false;
    old_live boolean := false;
    new_live boolean := false;
    old_oversize boolean := false;
    new_oversize boolean := false;
    marker_organization_id uuid;
BEGIN
    IF TG_OP = 'DELETE' THEN
        SELECT organization_id INTO marker_organization_id
        FROM model_hub_annotation_value_marker
        WHERE score_id = OLD.id;
        IF FOUND THEN
            PERFORM model_hub_annotation_organization_lock(marker_organization_id);
        END IF;
        DELETE FROM model_hub_annotation_value_marker
        WHERE score_id = OLD.id
        RETURNING blocked_oversize INTO old_oversize;
        claimed := FOUND;
        IF COALESCE(claimed, false) THEN
            old_live := NOT OLD.deleted
                AND OLD.tracer_project_id IS NOT NULL
                AND (OLD.trace_id IS NOT NULL OR OLD.observation_span_id IS NOT NULL);
            IF old_live AND NOT old_oversize THEN
                PERFORM model_hub_annotation_value_decrement(
                    OLD.tracer_project_id, OLD.label_id, OLD.value
                );
                PERFORM model_hub_annotation_label_revision_bump(OLD.label_id);
            END IF;
        END IF;
        RETURN OLD;
    END IF;

    new_relevant := (
        NEW.trace_id IS NOT NULL OR NEW.observation_span_id IS NOT NULL
    ) AND model_hub_annotation_label_is_categorical(NEW.label_id);
    IF TG_OP = 'UPDATE' THEN
        old_relevant := (
            OLD.trace_id IS NOT NULL OR OLD.observation_span_id IS NOT NULL
        ) AND model_hub_annotation_label_is_categorical(OLD.label_id);
    END IF;
    IF TG_OP = 'UPDATE' AND old_relevant THEN
        PERFORM model_hub_annotation_organization_lock(OLD.organization_id);
    END IF;
    IF new_relevant THEN
        PERFORM model_hub_annotation_organization_lock(NEW.organization_id);
    END IF;
    new_candidate := NOT NEW.deleted
        AND NEW.tracer_project_id IS NOT NULL
        AND new_relevant;
    IF TG_OP = 'UPDATE' THEN
        old_live := NOT OLD.deleted
            AND OLD.tracer_project_id IS NOT NULL
            AND old_relevant;
    END IF;
    IF new_candidate THEN
        new_scope_valid := model_hub_annotation_score_scope_is_valid(
            NEW.organization_id,
            NEW.tracer_project_id,
            NEW.label_id
        );
    END IF;
    new_live := new_candidate AND new_scope_valid;
    IF new_live THEN
        new_oversize := model_hub_annotation_payload_has_oversize(NEW.value);
    END IF;

    -- Readiness includes the invariant that every live categorical trace/span
    -- Score is project-scoped. Non-categorical Scores are intentionally outside
    -- this projection and cannot close its gate.
    IF (
        NOT NEW.deleted
        AND new_relevant
        AND (
            NEW.tracer_project_id IS NULL
            OR NOT new_scope_valid
        )
    ) OR new_oversize
    THEN
        INSERT INTO model_hub_annotation_value_status (
            organization_id, projection_version, ready,
            projected_scores, backfill_cursor, updated_at
        ) VALUES (NEW.organization_id, 1, false, 0, NULL, NOW())
        ON CONFLICT (organization_id) DO UPDATE SET
            ready = false,
            updated_at = NOW();
    END IF;
    IF new_live AND NOT new_oversize THEN
        PERFORM model_hub_annotation_status_ensure_exists(NEW.organization_id);
    END IF;

    -- Scores for non-categorical labels and non-observability sources never
    -- participate. Avoid growing the marker table on unrelated writes.
    IF NOT new_live AND (TG_OP = 'INSERT' OR NOT old_live) THEN
        RETURN NEW;
    END IF;

    -- A live categorical Score can move out of projection scope through soft
    -- deletion, project clearing, source clearing, or a label reassignment.
    -- Remove its exact contribution and marker atomically.
    IF TG_OP = 'UPDATE' AND old_live AND NOT new_live THEN
        DELETE FROM model_hub_annotation_value_marker
        WHERE score_id = OLD.id
        RETURNING blocked_oversize INTO old_oversize;
        claimed := FOUND;
        IF COALESCE(claimed, false) AND NOT old_oversize THEN
            PERFORM model_hub_annotation_value_decrement(
                OLD.tracer_project_id, OLD.label_id, OLD.value
            );
            PERFORM model_hub_annotation_label_revision_bump(OLD.label_id);
        END IF;
        RETURN NEW;
    END IF;

    INSERT INTO model_hub_annotation_value_marker (
        score_id, organization_id, projection_version, blocked_oversize
    )
    VALUES (NEW.id, NEW.organization_id, 1, new_oversize)
    ON CONFLICT (score_id) DO NOTHING
    RETURNING true INTO claimed;

    IF TG_OP = 'INSERT' THEN
        IF NOT COALESCE(claimed, false) THEN
            RAISE EXCEPTION 'annotation value marker already exists for new score'
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF new_live AND NOT new_oversize THEN
            PERFORM model_hub_annotation_value_increment(
                NEW.organization_id,
                NEW.tracer_project_id,
                NEW.label_id,
                NEW.value
            );
            PERFORM model_hub_annotation_label_revision_bump(NEW.label_id);
        END IF;
        RETURN NEW;
    END IF;

    -- A Score newly entering categorical trace/span scope must be a fresh
    -- marker claim; otherwise an out-of-scope row retained stale projection
    -- state and the invariant is already broken.
    IF NOT old_live AND new_live THEN
        IF NOT COALESCE(claimed, false) THEN
            RAISE EXCEPTION 'annotation value marker already exists for newly scoped score'
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        IF NOT new_oversize THEN
            PERFORM model_hub_annotation_value_increment(
                NEW.organization_id,
                NEW.tracer_project_id,
                NEW.label_id,
                NEW.value
            );
            PERFORM model_hub_annotation_label_revision_bump(NEW.label_id);
        END IF;
        RETURN NEW;
    END IF;

    IF NOT COALESCE(claimed, false) THEN
        SELECT blocked_oversize INTO old_oversize
        FROM model_hub_annotation_value_marker
        WHERE score_id = NEW.id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'annotation value marker disappeared during score update'
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        UPDATE model_hub_annotation_value_marker
        SET blocked_oversize = new_oversize
        WHERE score_id = NEW.id;
    END IF;

    -- An update can be the first touch of a pre-trigger historic Score.  The
    -- marker claim makes that update its atomic projection/backfill boundary.
    IF COALESCE(claimed, false) THEN
        IF new_live AND NOT new_oversize THEN
            PERFORM model_hub_annotation_value_increment(
                NEW.organization_id,
                NEW.tracer_project_id,
                NEW.label_id,
                NEW.value
            );
            PERFORM model_hub_annotation_label_revision_bump(NEW.label_id);
        END IF;
        RETURN NEW;
    END IF;

    IF ROW(
        OLD.deleted,
        OLD.tracer_project_id,
        OLD.label_id,
        OLD.trace_id,
        OLD.observation_span_id,
        OLD.value
    ) IS NOT DISTINCT FROM ROW(
        NEW.deleted,
        NEW.tracer_project_id,
        NEW.label_id,
        NEW.trace_id,
        NEW.observation_span_id,
        NEW.value
    ) THEN
        RETURN NEW;
    END IF;

    IF old_live AND NOT old_oversize THEN
        PERFORM model_hub_annotation_value_decrement(
            OLD.tracer_project_id, OLD.label_id, OLD.value
        );
    END IF;
    IF new_live AND NOT new_oversize THEN
        PERFORM model_hub_annotation_value_increment(
            NEW.organization_id,
            NEW.tracer_project_id,
            NEW.label_id,
            NEW.value
        );
    END IF;
    IF old_live AND NOT old_oversize THEN
        PERFORM model_hub_annotation_label_revision_bump(OLD.label_id);
    END IF;
    IF new_live
       AND NOT new_oversize
       AND (
           NOT old_live
           OR old_oversize
           OR NEW.label_id IS DISTINCT FROM OLD.label_id
       )
    THEN
        PERFORM model_hub_annotation_label_revision_bump(NEW.label_id);
    END IF;
    RETURN NEW;
END;
$function$;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DROP TRIGGER IF EXISTS model_hub_sync_annotation_score_value_trigger
ON model_hub_score;
CREATE CONSTRAINT TRIGGER model_hub_sync_annotation_score_value_trigger
AFTER INSERT OR UPDATE OR DELETE ON model_hub_score
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION model_hub_sync_annotation_score_value();
"""


UNINSTALL_SQL = r"""
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DROP TRIGGER IF EXISTS model_hub_sync_annotation_score_value_trigger
ON model_hub_score;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DROP TRIGGER IF EXISTS model_hub_annotation_label_type_immutable_trigger
ON model_hub_annotationslabels;
DROP FUNCTION IF EXISTS model_hub_sync_annotation_score_value();
DROP FUNCTION IF EXISTS model_hub_project_annotation_score_value(uuid);
DROP FUNCTION IF EXISTS model_hub_annotation_status_ensure_exists(uuid);
DROP FUNCTION IF EXISTS model_hub_annotation_label_revision_bump(uuid);
DROP FUNCTION IF EXISTS model_hub_annotation_value_decrement(uuid, uuid, jsonb);
DROP FUNCTION IF EXISTS model_hub_annotation_value_increment(uuid, uuid, uuid, jsonb);
DROP FUNCTION IF EXISTS model_hub_annotation_digest_guard(text, text, bigint);
DROP FUNCTION IF EXISTS model_hub_annotation_organization_lock(uuid);
DROP FUNCTION IF EXISTS model_hub_annotation_label_type_immutable();
DROP FUNCTION IF EXISTS model_hub_annotation_score_scope_is_valid(uuid, uuid, uuid);
DROP FUNCTION IF EXISTS model_hub_annotation_label_is_categorical(uuid);
DROP FUNCTION IF EXISTS model_hub_annotation_payload_has_oversize(jsonb);
DROP FUNCTION IF EXISTS model_hub_annotation_score_value_texts(jsonb);
DROP FUNCTION IF EXISTS model_hub_annotation_value_to_text(jsonb);
"""


class Migration(migrations.Migration):
    # A timeout rolls the whole install back, so the next deployment restarts
    # from a clean migration state instead of a partially installed trigger set.
    atomic = True
    dependencies = [("model_hub", "0122_backfill_queueitem_source_preview")]

    operations = [
        # Both extensions are shared infrastructure. Never drop them when this
        # one migration is reversed: pg_trgm already backs older model_hub
        # indexes and pgcrypto can be used by unrelated installations.
        migrations.RunSQL(
            sql="CREATE EXTENSION IF NOT EXISTS pgcrypto;",
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.RunSQL(
            sql="CREATE EXTENSION IF NOT EXISTS pg_trgm;",
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.CreateModel(
            name="AnnotationScoreValueMarker",
            fields=[
                ("score_id", models.UUIDField(primary_key=True, serialize=False)),
                ("organization_id", models.UUIDField()),
                (
                    "projection_version",
                    models.PositiveSmallIntegerField(default=PROJECTION_VERSION),
                ),
                ("blocked_oversize", models.BooleanField(default=False)),
            ],
            options={"db_table": "model_hub_annotation_value_marker"},
        ),
        migrations.CreateModel(
            name="AnnotationScoreValueLabelState",
            fields=[
                ("label_id", models.UUIDField(primary_key=True, serialize=False)),
                ("revision", models.BigIntegerField(default=0)),
            ],
            options={"db_table": "model_hub_annotation_value_label_state"},
        ),
        migrations.CreateModel(
            name="AnnotationScoreValueProjectionStatus",
            fields=[
                (
                    "organization_id",
                    models.UUIDField(primary_key=True, serialize=False),
                ),
                (
                    "projection_version",
                    models.PositiveSmallIntegerField(default=PROJECTION_VERSION),
                ),
                ("ready", models.BooleanField(default=False)),
                ("projected_scores", models.BigIntegerField(default=0)),
                ("backfill_cursor", models.UUIDField(blank=True, null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "model_hub_annotation_value_status"},
        ),
        migrations.CreateModel(
            name="AnnotationScoreValueVocabulary",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("tracer_project_id", models.UUIDField()),
                ("label_id", models.UUIDField()),
                ("value_digest", models.BinaryField(max_length=32)),
                ("value_text", models.TextField()),
                ("value_sort_prefix", models.CharField(max_length=384)),
                ("value_search", models.TextField()),
                ("ref_count", models.BigIntegerField()),
            ],
            options={
                "db_table": "model_hub_annotation_value_vocab",
                "indexes": [
                    models.Index(
                        fields=[
                            "tracer_project_id",
                            "label_id",
                            "value_sort_prefix",
                            "value_digest",
                        ],
                        name="annv_project_label_order_idx",
                    ),
                    django.contrib.postgres.indexes.GinIndex(
                        fields=["value_search"],
                        name="annv_value_search_trgm_idx",
                        opclasses=["gin_trgm_ops"],
                    ),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("tracer_project_id", "label_id", "value_digest"),
                        name="annv_project_label_digest_uniq",
                    ),
                    models.CheckConstraint(
                        condition=models.Q(("ref_count__gt", 0)),
                        name="annv_positive_ref_count",
                    ),
                ],
            },
        ),
        migrations.RunSQL(sql=INSTALL_SQL, reverse_sql=UNINSTALL_SQL),
    ]
