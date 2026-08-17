"""Indexed exact vocabulary projection for tracer annotation ``Score`` values.

The source ``model_hub_score.value`` is JSON and may contain a scalar, a list,
or one of the historic wrapper-object shapes.  Scanning that append-heavy table
to populate an interactive picker cannot meet the four-second request wall.
These small tables are maintained by the database trigger installed in
``0123_annotation_score_value_projection`` and are intentionally not written by
request code.
"""

from django.contrib.postgres.indexes import GinIndex
from django.db import models


class AnnotationScoreValueVocabulary(models.Model):
    """One live normalized value per tracer-project/annotation-label pair."""

    tracer_project_id = models.UUIDField()
    label_id = models.UUIDField()
    value_digest = models.BinaryField(max_length=32)
    value_text = models.TextField()
    # A bounded prefix keeps the ordered btree below PostgreSQL's index-entry
    # limit even when an imported annotation contains a very large string.
    value_sort_prefix = models.CharField(max_length=384)
    value_search = models.TextField()
    ref_count = models.BigIntegerField()

    class Meta:
        db_table = "model_hub_annotation_value_vocab"
        constraints = [
            models.UniqueConstraint(
                fields=["tracer_project_id", "label_id", "value_digest"],
                name="annv_project_label_digest_uniq",
            ),
            models.CheckConstraint(
                condition=models.Q(ref_count__gt=0),
                name="annv_positive_ref_count",
            ),
        ]
        indexes = [
            models.Index(
                fields=[
                    "tracer_project_id",
                    "label_id",
                    "value_sort_prefix",
                    "value_digest",
                ],
                name="annv_project_label_order_idx",
            ),
            GinIndex(
                fields=["value_search"],
                name="annv_value_search_trgm_idx",
                opclasses=["gin_trgm_ops"],
            ),
        ]


class AnnotationScoreValueMarker(models.Model):
    """Idempotence marker proving that one Score contribution was projected."""

    score_id = models.UUIDField(primary_key=True)
    organization_id = models.UUIDField()
    projection_version = models.PositiveSmallIntegerField(default=1)
    blocked_oversize = models.BooleanField(default=False)

    class Meta:
        db_table = "model_hub_annotation_value_marker"


class AnnotationScoreValueLabelState(models.Model):
    """Monotonic label revision used to reject changed-data cursors."""

    label_id = models.UUIDField(primary_key=True)
    revision = models.BigIntegerField(default=0)

    class Meta:
        db_table = "model_hub_annotation_value_label_state"


class AnnotationScoreValueProjectionStatus(models.Model):
    """Organization-local readiness for the resumable historical projection.

    Projection failures must never make another tenant's categorical picker
    unavailable.  The organization id is deliberately stored as a scalar
    rather than an FK: this is operational projection state and must remain
    readable while tenant deletion is being reconciled.
    """

    organization_id = models.UUIDField(primary_key=True)
    projection_version = models.PositiveSmallIntegerField(default=1)
    ready = models.BooleanField(default=False)
    projected_scores = models.BigIntegerField(default=0)
    backfill_cursor = models.UUIDField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "model_hub_annotation_value_status"
