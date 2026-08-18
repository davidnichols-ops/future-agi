from django.db import models


class HarnessSessionLink(models.Model):
    """Which platform run-test/execution a harness session belongs to.

    The harness knows nothing about platform entities; this mapping is the only
    place the two id spaces meet, and the proxy reads it to enrich responses.
    """

    session_id = models.CharField(max_length=128, unique=True)
    run_test_id = models.CharField(max_length=64, null=True, blank=True)
    execution_id = models.CharField(max_length=64, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "simulate_harness_session_link"
