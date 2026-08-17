"""Routable source for annotation-label discovery (which labels have scores in a project).

Routed via ``_REGISTRY["ANNOTATION_LABELS"]`` (see v2/dispatch.py):

- ``V1_ONLY`` → :class:`AnnotationLabelScoresPG` — joins ``Score`` through
  ``trace``/``observation_span`` (valid only while the legacy PG tables exist).
- ``V2_ONLY``/``V2_PRIMARY`` → :class:`AnnotationLabelScoresProjectPG` — filters
  ``Score`` on the denormalized ``tracer_project_id`` (valid post-CH25, cheap).

:class:`AnnotationLabelScoresCH` scopes the legacy CDC ``model_hub_score`` via
the direct-write CH25 ``spans`` table.  That is a cross-topology query: the two
tables are not co-located after the direct-write cutover.  Public reads are
therefore pinned to :class:`AnnotationLabelScoresProjectPG`, where ``Score`` is
authoritative and ``tracer_project_id`` is populated by every tracer Score
write (and backfilled for historic rows).

``label_ids_for_project(project_id) -> list[str]`` is the dispatched entrypoint
(all sources expose it) so the dispatcher stays backend-blind.

Note: ``Score.project``/``model_hub_score.project_id`` point at
``model_hub.DevelopAI`` (a different id space) and are NOT used for scoping; the
denormalized ``Score.tracer_project_id`` carries the ``tracer.Project`` id.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.db import DatabaseError, connection

ANNOTATION_SUGGESTION_VALUE_MAX_BYTES = 16 * 1024
ANNOTATION_SUGGESTION_PAYLOAD_MAX_BYTES = 64 * 1024
ANNOTATION_SUGGESTION_VALUES_PER_SCORE_MAX = 256
ANNOTATION_VALUE_PROJECT_BATCH_MAX = 64


class AnnotationScoreReadUnavailable(RuntimeError):
    """Stable boundary error for authoritative annotation Score reads."""


class AnnotationScoreVocabularyChanged(RuntimeError):
    """The exact vocabulary changed after a continuation was issued."""


@dataclass(frozen=True)
class AnnotationScoreValueRow:
    value: str
    sort_prefix: str
    digest: str

    @property
    def order(self) -> tuple[str, str]:
        # The SHA-256 digest is the stable uniqueness tie-break.  Do not put
        # the full (up to 16 KiB) value into signed URL cursors: an
        # incompressible permitted value would exceed the API's cursor length
        # limit and make its own exact continuation unusable.
        return (self.sort_prefix, self.digest)


@dataclass(frozen=True)
class AnnotationScoreValuePage:
    rows: tuple[AnnotationScoreValueRow, ...]
    revision: str
    has_more: bool


def _materialize_score_rows(queryset):
    """Evaluate a Score queryset without exposing database diagnostics."""

    try:
        return list(queryset)
    except DatabaseError:
        raise AnnotationScoreReadUnavailable(
            "Annotation score data is temporarily unavailable"
        ) from None


class AnnotationLabelScoresPG:
    """v1: label ids of scores in a project, via PG joins (legacy tables)."""

    def label_ids_for_project(self, project_id) -> list[str]:
        from django.db.models import Q

        from model_hub.models.score import Score

        return [
            str(lid)
            for lid in Score.no_workspace_objects.filter(
                Q(trace__project_id=project_id)
                | Q(observation_span__project_id=project_id),
            )
            .order_by()
            .values_list("label_id", flat=True)
            .distinct()
            if lid
        ]


# Scope model_hub_score (s) to tracer projects via spans. Param: project_ids (list[str]).
_CH_PROJECT_SCOPE = """(
        (isNotNull(s.trace_id) AND toString(s.trace_id) IN (
            SELECT DISTINCT trace_id FROM spans
            WHERE project_id IN %(project_ids)s AND is_deleted = 0
        ))
        OR (s.observation_span_id IN (
            SELECT DISTINCT id FROM spans
            WHERE project_id IN %(project_ids)s AND is_deleted = 0
        ))
    )"""


class AnnotationLabelScoresProjectPG:
    """Direct-write-safe Score reads scoped by ``Score.tracer_project_id``.

    Replaces the CH ``spans``-scoped scan (see :class:`AnnotationLabelScoresCH`),
    which is invalid once legacy CDC tables and direct-write CH25 spans live on
    different clusters.  The trace/span not-null predicate keeps session-only
    and non-observability scores out, matching the old public response.
    """

    FILTER_VALUE_LIMIT = 5_000
    GRAPH_EVENT_LIMIT = 2_001

    @staticmethod
    def _trace_span_scope():
        from django.db.models import Q

        return Q(trace_id__isnull=False) | Q(observation_span_id__isnull=False)

    def label_ids_for_project(self, project_id) -> list[str]:
        from model_hub.models.score import Score

        # no_workspace_objects: project is the tenancy boundary (matching the CH
        # source and every write-site); this manager already applies deleted=False.
        # .order_by() defensively strips any default ordering so it can't leak into
        # SELECT DISTINCT and break the per-label dedup. Score._meta.ordering is []
        # today (it does not inherit BaseModel's "-created_at"), so this is a guard.
        rows = _materialize_score_rows(
            Score.no_workspace_objects.filter(
                self._trace_span_scope(),
                tracer_project_id=project_id,
            )
            .order_by()
            .values_list("label_id", flat=True)
            .distinct()
        )
        return [str(lid) for lid in rows if lid]

    def label_ids_by_project(self, project_ids: list[str]) -> dict[str, list[str]]:
        """Return label ids grouped by their authoritative tracer project.

        Organization list endpoints must not flatten this relation into one
        label union: ``has_annotation`` means completeness against the labels
        configured for *that row's* project.  Reading all requested projects
        in one finite metadata query avoids an N+1 while preserving that
        tenant boundary.
        """

        normalized = tuple(dict.fromkeys(str(value) for value in project_ids if value))
        grouped: dict[str, list[str]] = {project_id: [] for project_id in normalized}
        if not normalized:
            return grouped

        from model_hub.models.score import Score

        rows = _materialize_score_rows(
            Score.no_workspace_objects.filter(
                self._trace_span_scope(),
                tracer_project_id__in=normalized,
            )
            .order_by()
            .values_list("tracer_project_id", "label_id")
            .distinct()
        )
        for project_id, label_id in rows:
            project_key = str(project_id)
            if project_key in grouped and label_id:
                grouped[project_key].append(str(label_id))
        return grouped

    def label_has_scores_for_projects(self, label_id, project_ids: list[str]) -> bool:
        """Return whether one label is visible in any requested tracer project.

        Workspace-level annotation labels are only project-visible after an
        observability Score binds them to that project.  Keep the exact-value
        endpoint aligned with the property catalog without materializing every
        distinct label in a high-volume project: the existing
        ``(tracer_project_id, label)`` index can stop after the first match.
        """

        normalized = tuple(dict.fromkeys(str(value) for value in project_ids if value))
        if not normalized:
            return False

        from model_hub.models.score import Score

        rows = _materialize_score_rows(
            Score.no_workspace_objects.filter(
                self._trace_span_scope(),
                tracer_project_id__in=normalized,
                label_id=label_id,
            )
            .order_by()
            .values_list("id", flat=True)[:1]
        )
        return bool(rows)

    def annotator_ids_for_projects(self, project_ids: list[str]) -> list[str]:
        """Return distinct annotators for the requested tracer projects only."""

        if not project_ids:
            return []

        from django.db.models import Exists, OuterRef

        from accounts.models.user import User
        from model_hub.models.score import Score

        # Drive the query from the much smaller User relation and use the
        # implicit Score.annotator FK index for a stop-at-first-match EXISTS.
        # A DISTINCT scan over every Score in a heavy project is exactly the
        # unbounded shape this source replaces.
        matching_score = Score.no_workspace_objects.filter(
            self._trace_span_scope(),
            tracer_project_id__in=project_ids,
            annotator_id=OuterRef("pk"),
        )
        rows = _materialize_score_rows(
            User.objects.filter(Exists(matching_score))
            .order_by()
            .values_list("id", flat=True)
        )
        return [str(annotator_id) for annotator_id in rows if annotator_id]

    def annotator_page_for_projects(
        self,
        project_ids: list[str],
        *,
        page_size: int,
        search: str = "",
        after_id=None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Return one exact annotator page using the User primary-key order.

        The outer keyset is backed by ``accounts_user_pkey``.  Its correlated
        existence check is backed by Django's implicit ``Score.annotator`` FK
        index and retains the authoritative tracer-project predicate.  This is
        deliberately driven from the much smaller User relation: driving from
        project Scores would require an unindexed ``(project, annotator)``
        distinct/order scan.
        """

        finite_page_size = int(page_size)
        if finite_page_size < 1 or finite_page_size > 50:
            raise ValueError("page_size must be between 1 and 50")
        if not project_ids:
            return [], False

        users = self._annotator_queryset_for_projects(
            project_ids,
            search=search,
            after_id=after_id,
        )
        rows = _materialize_score_rows(
            users.values("id", "name", "email")[: finite_page_size + 1]
        )
        has_more = len(rows) > finite_page_size
        return rows[:finite_page_size], has_more

    def _annotator_queryset_for_projects(
        self,
        project_ids: list[str],
        *,
        search: str = "",
        after_id=None,
    ):
        """Build the index-keyed annotator query without evaluating it."""

        from django.db.models import Exists, OuterRef, Q

        from accounts.models.user import User
        from model_hub.models.score import Score

        matching_score = Score.no_workspace_objects.filter(
            self._trace_span_scope(),
            tracer_project_id__in=project_ids,
            annotator_id=OuterRef("pk"),
        )
        users = User.objects.filter(Exists(matching_score))
        if after_id is not None:
            users = users.filter(id__gt=after_id)
        normalized_search = str(search or "").strip()
        if normalized_search:
            search_filter = Q(name__icontains=normalized_search) | Q(
                email__icontains=normalized_search
            )
            try:
                from uuid import UUID

                search_filter |= Q(pk=UUID(normalized_search))
            except (TypeError, ValueError):
                pass
            users = users.filter(search_filter)
        return users.order_by("id")

    def categorical_values_for_label(
        self, label_id, project_ids: list[str]
    ) -> list[Any]:
        """Return stored Score JSON values only when the bounded read exhausts.

        The deployed ``(tracer_project_id, label_id)`` index can stop an
        unordered scan after ``FILTER_VALUE_LIMIT + 1`` rows, but it cannot
        support an exact value cursor.  Reading one sentinel row lets callers
        distinguish a small, exhaustive vocabulary source from a truncated
        sample.  Large histories fail closed instead of being presented as a
        successful sampled picker response.
        """

        if not project_ids:
            return []

        from model_hub.models.score import Score

        # The picker deduplicates categorical choices, so ordering millions of
        # Score rows adds work without helping completeness.  The sentinel read
        # stays index-bounded and proves exhaustion when it returns at most the
        # configured cap.
        rows = _materialize_score_rows(
            Score.no_workspace_objects.filter(
                self._trace_span_scope(),
                tracer_project_id__in=project_ids,
                label_id=label_id,
            )
            .order_by()
            .values_list("value", flat=True)[: self.FILTER_VALUE_LIMIT + 1]
        )
        if len(rows) > self.FILTER_VALUE_LIMIT:
            raise AnnotationScoreReadUnavailable(
                "Annotation score vocabulary exceeds the bounded exact-read limit"
            )
        return [value for value in rows if value not in (None, "")]

    def categorical_value_page_for_label(
        self,
        label_id,
        project_ids: list[str],
        *,
        page_size: int,
        search: str = "",
        after: tuple[str, str] | None = None,
        expected_revision: str | None = None,
        excluded_values: tuple[str, ...] = (),
    ) -> AnnotationScoreValuePage:
        """Read one exact keyset page from the trigger-maintained projection.

        The query touches the tiny readiness/label-state rows and at most one
        indexed vocabulary page.  ``expected_revision`` binds every
        continuation to the complete label vocabulary (including projects in
        later authorization batches), so a concurrent Score change is rejected
        instead of silently skipping or repeating a value.
        """

        finite_page_size = int(page_size)
        if finite_page_size < 1 or finite_page_size > 50:
            raise ValueError("page_size must be between 1 and 50")
        if not project_ids:
            # Still require projection readiness: returning a successful empty
            # page while historical backfill is incomplete would make an empty
            # authorized batch indistinguishable from an unready source.
            normalized_projects: tuple[str, ...] = ()
        else:
            normalized_projects = tuple(
                dict.fromkeys(str(value) for value in project_ids if value)
            )
        if len(normalized_projects) > ANNOTATION_VALUE_PROJECT_BATCH_MAX:
            raise ValueError(
                "annotation value project batch cannot exceed "
                f"{ANNOTATION_VALUE_PROJECT_BATCH_MAX} projects"
            )

        normalized_after = None
        if after is not None:
            if (
                not isinstance(after, tuple)
                or len(after) != 2
                or any(not isinstance(value, str) for value in after)
                or len(after[1]) != 64
                or any(character not in "0123456789abcdef" for character in after[1])
            ):
                raise ValueError("invalid annotation value keyset")
            normalized_after = after

        predicates = [
            "v.label_id = %s",
            "v.ref_count > 0",
        ]
        parameters: list[Any] = [str(label_id)]

        normalized_search = str(search or "").strip().lower()
        if normalized_search:
            escaped_search = (
                normalized_search.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )
            predicates.append("v.value_search LIKE %s ESCAPE E'\\\\'")
            parameters.append(f"%{escaped_search}%")

        normalized_excluded = tuple(
            dict.fromkeys(str(value) for value in excluded_values)
        )
        if normalized_excluded:
            predicates.append("NOT (v.value_text = ANY(%s::text[]))")
            parameters.append(list(normalized_excluded))

        if normalized_after is not None:
            predicates.append(
                "(v.value_sort_prefix, v.value_digest) > (%s, decode(%s, 'hex'))"
            )
            parameters.extend(normalized_after)

        query = f"""
            WITH requested_projects AS (
                SELECT unnest(%s::uuid[]) AS project_id
            ), projection_state AS (
                SELECT
                    NOT EXISTS (
                        SELECT 1
                        FROM requested_projects AS requested
                        LEFT JOIN tracer_project AS project
                          ON project.id = requested.project_id
                         AND NOT project.deleted
                        LEFT JOIN model_hub_annotation_value_status AS status
                          ON status.organization_id = project.organization_id
                        WHERE project.id IS NULL
                           OR status.organization_id IS NULL
                           OR NOT status.ready
                           OR status.projection_version <> 1
                    ) AS ready,
                    1 AS projection_version,
                    COALESCE(label_state.revision, 0) AS revision
                FROM (SELECT 1) AS singleton
                LEFT JOIN model_hub_annotation_value_label_state AS label_state
                  ON label_state.label_id = %s
            ), project_candidates AS (
                SELECT
                    requested.project_id,
                    candidate.value_text,
                    candidate.value_sort_prefix,
                    candidate.value_digest
                FROM requested_projects AS requested
                CROSS JOIN LATERAL (
                    SELECT
                        v.value_text,
                        v.value_sort_prefix,
                        v.value_digest
                    FROM model_hub_annotation_value_vocab AS v
                    WHERE v.tracer_project_id = requested.project_id
                      AND {" AND ".join(predicates)}
                    ORDER BY v.value_sort_prefix, v.value_digest, v.value_text
                    LIMIT %s
                ) AS candidate
            ), project_more AS (
                SELECT EXISTS (
                    SELECT 1
                    FROM project_candidates
                    GROUP BY project_id
                    HAVING count(*) > %s
                ) AS has_more
            ), page_values AS (
                SELECT
                    v.value_text,
                    v.value_sort_prefix,
                    encode(v.value_digest, 'hex') AS value_digest
                FROM project_candidates AS v
                GROUP BY v.value_text, v.value_sort_prefix, v.value_digest
                ORDER BY v.value_sort_prefix, v.value_digest, v.value_text
                LIMIT %s
            )
            SELECT
                state.ready,
                state.projection_version,
                state.revision,
                project_more.has_more,
                page.value_text,
                page.value_sort_prefix,
                page.value_digest
            FROM projection_state AS state
            CROSS JOIN project_more
            LEFT JOIN page_values AS page ON true
            ORDER BY
                page.value_sort_prefix NULLS LAST,
                page.value_digest NULLS LAST,
                page.value_text NULLS LAST
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    [
                        list(normalized_projects),
                        str(label_id),
                        *parameters,
                        finite_page_size + 1,
                        finite_page_size,
                        finite_page_size + 1,
                    ],
                )
                result = list(cursor.fetchall())
        except DatabaseError:
            raise AnnotationScoreReadUnavailable(
                "Annotation score vocabulary is temporarily unavailable"
            ) from None

        if not result or not bool(result[0][0]) or int(result[0][1]) != 1:
            raise AnnotationScoreReadUnavailable(
                "Annotation score vocabulary projection is not ready"
            )
        revision = f"{int(result[0][1])}:{int(result[0][2])}"
        if expected_revision is not None and expected_revision != revision:
            raise AnnotationScoreVocabularyChanged(
                "The annotation values changed while this cursor was active"
            )

        value_rows = tuple(
            AnnotationScoreValueRow(str(row[4]), str(row[5]), str(row[6]))
            for row in result
            if row[4] is not None
        )
        has_more = bool(result[0][3]) or len(value_rows) > finite_page_size
        return AnnotationScoreValuePage(
            value_rows[:finite_page_size],
            revision,
            has_more,
        )

    def annotation_rows_for_candidates(
        self,
        *,
        project_id: str,
        label_id: str,
        start_date,
        end_date,
        trace_ids: tuple[str, ...] = (),
        span_entities: tuple[tuple[str, str], ...] = (),
        limit: int = GRAPH_EVENT_LIMIT,
    ) -> list[dict[str, Any]]:
        """Read a finite annotation slice for already-proven CH25 candidates.

        ``Score`` remains the annotation source of truth in PostgreSQL; only
        the candidate trace/span identities originate in CH25.  Project and
        label predicates hit ``idx_score_tracer_project_label`` before the
        finite entity predicate.  Span matching uses the trace/span pair, not
        a bare OTel span id, so collisions cannot cross candidate traces.
        """

        from django.db.models import CharField, Q, Value
        from django.db.models.functions import Cast, Concat

        from model_hub.models.score import Score

        unique_trace_ids = tuple(
            dict.fromkeys(str(value) for value in trace_ids if value)
        )
        unique_span_entities = tuple(
            dict.fromkeys(
                (str(trace_id), str(span_id))
                for trace_id, span_id in span_entities
                if trace_id and span_id
            )
        )
        if not unique_trace_ids and not unique_span_entities:
            return []

        # Trace UUIDs have a canonical fixed-width string representation.  The
        # separator therefore produces an unambiguous composite candidate key
        # while keeping the query parameterized and ORM-owned.
        span_keys = tuple(
            f"{trace_id}:{span_id}" for trace_id, span_id in unique_span_entities
        )
        queryset = Score.no_workspace_objects.filter(
            tracer_project_id=project_id,
            label_id=label_id,
            created_at__gte=start_date,
            created_at__lt=end_date,
        )
        entity_scope = Q()
        if unique_trace_ids:
            entity_scope |= Q(trace_id__in=unique_trace_ids)
        if span_keys:
            queryset = queryset.annotate(
                _candidate_entity=Concat(
                    Cast("trace_id", output_field=CharField()),
                    Value(":"),
                    "observation_span_id",
                    output_field=CharField(),
                )
            )
            # The simple IN predicates let PostgreSQL discard unrelated rows
            # before evaluating the exact composite key.
            entity_scope |= Q(
                trace_id__in=tuple(pair[0] for pair in unique_span_entities),
                observation_span_id__in=tuple(pair[1] for pair in unique_span_entities),
                _candidate_entity__in=span_keys,
            )

        finite_limit = min(max(int(limit), 1), self.GRAPH_EVENT_LIMIT)
        return _materialize_score_rows(
            queryset.filter(entity_scope)
            .order_by("created_at", "id")
            .values("created_at", "value")[:finite_limit]
        )


class AnnotationLabelScoresCH:
    """Legacy CDC compatibility reader; not used by direct-write public APIs.

    Kept for legacy parity tests and rollback tooling only.  Direct-write paths
    must use :class:`AnnotationLabelScoresProjectPG` so they never try to join
    legacy ``model_hub_score`` to CH25 ``spans`` across clusters.
    """

    _QUERY = f"""
        SELECT DISTINCT toString(label_id) AS label_id
        FROM model_hub_score AS s FINAL
        WHERE s.deleted = false
          AND s._peerdb_is_deleted = 0
          AND {_CH_PROJECT_SCOPE}
    """

    def label_ids_for_project(self, project_id) -> list[str]:
        from tracer.services.clickhouse.client import get_clickhouse_client

        rows, _types, _ms = get_clickhouse_client().execute_read(
            self._QUERY, {"project_ids": [str(project_id)]}, timeout_ms=30000
        )
        return [r[0] for r in rows if r and r[0]]

    def annotator_ids_for_projects(self, project_ids: list[str]) -> list[str]:
        if not project_ids:
            return []
        from tracer.services.clickhouse.client import get_clickhouse_client

        query = f"""
            SELECT DISTINCT toString(annotator_id) AS annotator_id
            FROM model_hub_score AS s FINAL
            WHERE s.deleted = false AND s._peerdb_is_deleted = 0
              AND isNotNull(s.annotator_id)
              AND {_CH_PROJECT_SCOPE}
        """
        rows, _t, _ms = get_clickhouse_client().execute_read(
            query, {"project_ids": [str(p) for p in project_ids]}, timeout_ms=30000
        )
        return [r[0] for r in rows if r and r[0]]

    def categorical_values_for_label(
        self, label_id, project_ids: list[str]
    ) -> list[str]:
        if not project_ids:
            return []
        from tracer.services.clickhouse.client import get_clickhouse_client

        query = f"""
            SELECT value FROM model_hub_score AS s FINAL
            WHERE s.deleted = false AND s._peerdb_is_deleted = 0
              AND toString(s.label_id) = %(label_id)s
              AND {_CH_PROJECT_SCOPE}
            ORDER BY s.updated_at DESC
            LIMIT 5000
        """
        rows, _t, _ms = get_clickhouse_client().execute_read(
            query,
            {"label_id": str(label_id), "project_ids": [str(p) for p in project_ids]},
            timeout_ms=30000,
        )
        return [r[0] for r in rows if r and r[0] not in (None, "")]
