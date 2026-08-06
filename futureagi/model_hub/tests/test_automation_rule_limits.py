from types import SimpleNamespace
from unittest.mock import patch

from model_hub.utils import annotation_queue_helpers as helpers


def test_filter_mode_overflow_fails_before_queue_access():
    rule = SimpleNamespace(source_type="trace")

    with patch.object(helpers, "get_fk_field_name", return_value="trace"):
        result = helpers._add_source_ids_to_queue(
            rule,
            source_ids=["trace-1"],
            total_matching=helpers.AUTOMATION_RULE_MATCH_LIMIT + 1,
        )

    assert result == {
        "matched": helpers.AUTOMATION_RULE_MATCH_LIMIT + 1,
        "added": 0,
        "duplicates": 0,
        "truncated": True,
        "error": helpers.AUTOMATION_RULE_MATCH_LIMIT_ERROR,
    }


def test_filter_mode_overflow_preview_is_explicitly_truncated():
    rule = SimpleNamespace(source_type="trace")

    with patch.object(helpers, "get_fk_field_name", return_value="trace"):
        result = helpers._add_source_ids_to_queue(
            rule,
            source_ids=["trace-1"],
            total_matching=helpers.AUTOMATION_RULE_MATCH_LIMIT + 1,
            dry_run=True,
        )

    assert result == {
        "matched": helpers.AUTOMATION_RULE_MATCH_LIMIT + 1,
        "added": 0,
        "duplicates": 0,
        "truncated": True,
    }
