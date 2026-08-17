"""Install the online mutation fence for exact dataset-table pagination.

The migration deliberately does not scan Dataset/Row.  It installs the small
ledger first and commits every hot-table trigger independently so PostgreSQL
does not retain write-conflicting trigger DDL locks for the whole migration.
Historical datasets remain ``is_ready = false`` until the bounded operator
backfill serializes with the triggers and publishes their exact row count.

Each trigger DDL operation has a three-second lock timeout and a finite
statement timeout.  It therefore either acquires the one target-table lock and
commits independently, or fails the migration job so the idempotent operation
can be retried later; it never waits indefinitely behind a long writer while
blocking newer writes in PostgreSQL's lock queue. A mutation that lands before
its table's trigger is installed can never become reader-visible from this
interval: its ledger is missing/unready, and the later activation count
includes the committed source state. Dataset triggers are last so only
post-install Datasets may start ready.

This file is an in-place pre-release rewrite and assumes migration 0124 has not
been applied in any environment. An environment that applied an older 0124
must use a new repair migration; Django will not replay this changed file.
"""

from django.db import migrations

DDL_LOCK_TIMEOUT = "3s"
DDL_STATEMENT_TIMEOUT = "15s"


def _bounded_ddl(statement: str) -> str:
    """Bound one autocommitted migration operation's lock and execution wall.

    Successful operations reset the session settings before the migration
    proceeds.  A timeout aborts the one-shot migration process; the next
    operator retry starts on a fresh connection and safely replays the
    idempotent CREATE/REPLACE statement.
    """

    return (
        f"SET lock_timeout = '{DDL_LOCK_TIMEOUT}';\n"
        f"SET statement_timeout = '{DDL_STATEMENT_TIMEOUT}';\n"
        f"{statement.rstrip().rstrip(';')};\n"
        "RESET statement_timeout;\n"
        "RESET lock_timeout;"
    )


SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS model_hub_dataset_table_revision (
    dataset_id uuid PRIMARY KEY
        REFERENCES model_hub_dataset(id) ON DELETE CASCADE,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision >= 1),
    active_rows bigint NOT NULL DEFAULT 0 CHECK (active_rows >= 0),
    is_ready boolean NOT NULL DEFAULT false,
    ready_at timestamptz NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
)
"""

FUNCTIONS_SQL = r"""
CREATE OR REPLACE FUNCTION model_hub_dataset_revision_dataset_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Datasets created after all triggers are installed start from an exact
    -- empty state. Their later Row/Column/Cell writes update this same row.
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT id, 1, 0, true, clock_timestamp(), clock_timestamp()
    FROM new_datasets
    ON CONFLICT (dataset_id) DO UPDATE
    SET revision = model_hub_dataset_table_revision.revision + 1,
        is_ready = false,
        ready_at = NULL,
        updated_at = clock_timestamp();
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_dataset_update()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.id, 1, 0, false, NULL, clock_timestamp()
    FROM (
        SELECT id FROM old_datasets
        UNION
        SELECT id FROM new_datasets
    ) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.id
    ORDER BY affected.id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        updated_at = clock_timestamp()
    FROM (
        SELECT id FROM old_datasets
        UNION
        SELECT id FROM new_datasets
    ) AS affected
    WHERE ledger.dataset_id = affected.id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_rows_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Creating the not-ready sentinel is part of the source mutation.  This
    -- closes the race where a backfill could otherwise count while an older
    -- transaction had already missed an absent ledger row.
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (SELECT DISTINCT dataset_id FROM new_rows) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        active_rows = CASE
            WHEN ledger.is_ready THEN ledger.active_rows + affected.active_delta
            ELSE ledger.active_rows
        END,
        updated_at = clock_timestamp()
    FROM (
        SELECT dataset_id,
               COUNT(*) FILTER (WHERE NOT deleted)::bigint AS active_delta
        FROM new_rows
        GROUP BY dataset_id
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_rows_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (SELECT DISTINCT dataset_id FROM old_rows) AS affected
    -- During a physical Dataset cascade the parent is already absent.  Do not
    -- recreate a child ledger row whose FK would prevent that deletion.
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        active_rows = CASE
            WHEN ledger.is_ready THEN ledger.active_rows + affected.active_delta
            ELSE ledger.active_rows
        END,
        updated_at = clock_timestamp()
    FROM (
        SELECT dataset_id,
               -(COUNT(*) FILTER (WHERE NOT deleted))::bigint AS active_delta
        FROM old_rows
        GROUP BY dataset_id
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_rows_update()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (
        SELECT dataset_id FROM old_rows
        UNION
        SELECT dataset_id FROM new_rows
    ) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        active_rows = CASE
            WHEN ledger.is_ready THEN ledger.active_rows + affected.active_delta
            ELSE ledger.active_rows
        END,
        updated_at = clock_timestamp()
    FROM (
        SELECT dataset_id, SUM(active_delta)::bigint AS active_delta
        FROM (
            SELECT dataset_id,
                   -(COUNT(*) FILTER (WHERE NOT deleted))::bigint AS active_delta
            FROM old_rows
            GROUP BY dataset_id
            UNION ALL
            SELECT dataset_id,
                   COUNT(*) FILTER (WHERE NOT deleted)::bigint AS active_delta
            FROM new_rows
            GROUP BY dataset_id
        ) AS row_changes
        GROUP BY dataset_id
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_columns_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (
        SELECT DISTINCT dataset_id FROM new_columns WHERE dataset_id IS NOT NULL
    ) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        updated_at = clock_timestamp()
    FROM (
        SELECT DISTINCT dataset_id FROM new_columns WHERE dataset_id IS NOT NULL
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_columns_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (
        SELECT DISTINCT dataset_id FROM old_columns WHERE dataset_id IS NOT NULL
    ) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        updated_at = clock_timestamp()
    FROM (
        SELECT DISTINCT dataset_id FROM old_columns WHERE dataset_id IS NOT NULL
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_columns_update()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (
        SELECT dataset_id FROM old_columns WHERE dataset_id IS NOT NULL
        UNION
        SELECT dataset_id FROM new_columns WHERE dataset_id IS NOT NULL
    ) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        updated_at = clock_timestamp()
    FROM (
        SELECT dataset_id FROM old_columns WHERE dataset_id IS NOT NULL
        UNION
        SELECT dataset_id FROM new_columns WHERE dataset_id IS NOT NULL
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_cells_insert()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (
        SELECT DISTINCT dataset_row.dataset_id
        FROM new_cells AS changed_cell
        INNER JOIN model_hub_row AS dataset_row ON dataset_row.id = changed_cell.row_id
    ) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        updated_at = clock_timestamp()
    FROM (
        SELECT DISTINCT dataset_row.dataset_id
        FROM new_cells AS changed_cell
        INNER JOIN model_hub_row AS dataset_row ON dataset_row.id = changed_cell.row_id
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_cells_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (
        SELECT DISTINCT dataset_row.dataset_id
        FROM old_cells AS changed_cell
        INNER JOIN model_hub_row AS dataset_row ON dataset_row.id = changed_cell.row_id
    ) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        updated_at = clock_timestamp()
    FROM (
        SELECT DISTINCT dataset_row.dataset_id
        FROM old_cells AS changed_cell
        INNER JOIN model_hub_row AS dataset_row ON dataset_row.id = changed_cell.row_id
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;

CREATE OR REPLACE FUNCTION model_hub_dataset_revision_cells_update()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO model_hub_dataset_table_revision (
        dataset_id, revision, active_rows, is_ready, ready_at, updated_at
    )
    SELECT affected.dataset_id, 1, 0, false, NULL, clock_timestamp()
    FROM (
        SELECT dataset_row.dataset_id
        FROM old_cells AS changed_cell
        INNER JOIN model_hub_row AS dataset_row ON dataset_row.id = changed_cell.row_id
        UNION
        SELECT dataset_row.dataset_id
        FROM new_cells AS changed_cell
        INNER JOIN model_hub_row AS dataset_row ON dataset_row.id = changed_cell.row_id
    ) AS affected
    INNER JOIN model_hub_dataset AS dataset ON dataset.id = affected.dataset_id
    ORDER BY affected.dataset_id
    ON CONFLICT (dataset_id) DO NOTHING;

    UPDATE model_hub_dataset_table_revision AS ledger
    SET revision = ledger.revision + 1,
        updated_at = clock_timestamp()
    FROM (
        SELECT dataset_row.dataset_id
        FROM old_cells AS changed_cell
        INNER JOIN model_hub_row AS dataset_row ON dataset_row.id = changed_cell.row_id
        UNION
        SELECT dataset_row.dataset_id
        FROM new_cells AS changed_cell
        INNER JOIN model_hub_row AS dataset_row ON dataset_row.id = changed_cell.row_id
    ) AS affected
    WHERE ledger.dataset_id = affected.dataset_id;
    RETURN NULL;
END
$function$;
"""

TRIGGER_SQL = (
    (
        "dataset_revision_rows_insert",
        "model_hub_row",
        """CREATE OR REPLACE TRIGGER dataset_revision_rows_insert
AFTER INSERT ON model_hub_row
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_rows_insert()""",
    ),
    (
        "dataset_revision_rows_delete",
        "model_hub_row",
        """CREATE OR REPLACE TRIGGER dataset_revision_rows_delete
AFTER DELETE ON model_hub_row
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_rows_delete()""",
    ),
    (
        "dataset_revision_rows_update",
        "model_hub_row",
        """CREATE OR REPLACE TRIGGER dataset_revision_rows_update
AFTER UPDATE ON model_hub_row
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_rows_update()""",
    ),
    (
        "dataset_revision_columns_insert",
        "model_hub_column",
        """CREATE OR REPLACE TRIGGER dataset_revision_columns_insert
AFTER INSERT ON model_hub_column
REFERENCING NEW TABLE AS new_columns
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_columns_insert()""",
    ),
    (
        "dataset_revision_columns_delete",
        "model_hub_column",
        """CREATE OR REPLACE TRIGGER dataset_revision_columns_delete
AFTER DELETE ON model_hub_column
REFERENCING OLD TABLE AS old_columns
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_columns_delete()""",
    ),
    (
        "dataset_revision_columns_update",
        "model_hub_column",
        """CREATE OR REPLACE TRIGGER dataset_revision_columns_update
AFTER UPDATE ON model_hub_column
REFERENCING OLD TABLE AS old_columns NEW TABLE AS new_columns
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_columns_update()""",
    ),
    (
        "dataset_revision_cells_insert",
        "model_hub_cell",
        """CREATE OR REPLACE TRIGGER dataset_revision_cells_insert
AFTER INSERT ON model_hub_cell
REFERENCING NEW TABLE AS new_cells
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_cells_insert()""",
    ),
    (
        "dataset_revision_cells_delete",
        "model_hub_cell",
        """CREATE OR REPLACE TRIGGER dataset_revision_cells_delete
AFTER DELETE ON model_hub_cell
REFERENCING OLD TABLE AS old_cells
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_cells_delete()""",
    ),
    (
        "dataset_revision_cells_update",
        "model_hub_cell",
        """CREATE OR REPLACE TRIGGER dataset_revision_cells_update
AFTER UPDATE ON model_hub_cell
REFERENCING OLD TABLE AS old_cells NEW TABLE AS new_cells
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_cells_update()""",
    ),
    # Install Dataset triggers last, with INSERT absolutely last. A Dataset
    # created after this point can be published ready immediately because all
    # dependent-table triggers and the Dataset UPDATE trigger are active.
    (
        "dataset_revision_dataset_update",
        "model_hub_dataset",
        """CREATE OR REPLACE TRIGGER dataset_revision_dataset_update
AFTER UPDATE ON model_hub_dataset
REFERENCING OLD TABLE AS old_datasets NEW TABLE AS new_datasets
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_dataset_update()""",
    ),
    (
        "dataset_revision_dataset_insert",
        "model_hub_dataset",
        """CREATE OR REPLACE TRIGGER dataset_revision_dataset_insert
AFTER INSERT ON model_hub_dataset
REFERENCING NEW TABLE AS new_datasets
FOR EACH STATEMENT
EXECUTE FUNCTION model_hub_dataset_revision_dataset_insert()""",
    ),
)

DROP_FUNCTIONS_SQL = r"""
DROP FUNCTION IF EXISTS model_hub_dataset_revision_cells_update();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_cells_delete();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_cells_insert();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_columns_update();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_columns_delete();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_columns_insert();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_rows_update();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_rows_delete();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_rows_insert();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_dataset_update();
DROP FUNCTION IF EXISTS model_hub_dataset_revision_dataset_insert()
"""

INSTALL_SQL = "\n\n".join(
    (
        _bounded_ddl(SCHEMA_SQL),
        _bounded_ddl(FUNCTIONS_SQL),
        *(_bounded_ddl(statement) for _, _, statement in TRIGGER_SQL),
    )
)
REVERSE_SQL = "\n".join(
    (
        *(
            f"DROP TRIGGER IF EXISTS {name} ON {table};"
            for name, table, _ in TRIGGER_SQL
        ),
        DROP_FUNCTIONS_SQL,
        "DROP TABLE IF EXISTS model_hub_dataset_table_revision;",
    )
)


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("model_hub", "0123_annotation_score_value_projection"),
    ]

    operations = [
        migrations.RunSQL(
            [_bounded_ddl(SCHEMA_SQL)],
            [_bounded_ddl("DROP TABLE IF EXISTS model_hub_dataset_table_revision")],
        ),
        migrations.RunSQL(
            [_bounded_ddl(FUNCTIONS_SQL)],
            [_bounded_ddl(DROP_FUNCTIONS_SQL)],
        ),
        *(
            migrations.RunSQL(
                [_bounded_ddl(statement)],
                [_bounded_ddl(f"DROP TRIGGER IF EXISTS {name} ON {table}")],
            )
            for name, table, statement in TRIGGER_SQL
        ),
    ]
