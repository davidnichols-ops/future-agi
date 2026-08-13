from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from tracer.services.clickhouse.v2.apply_schema_rewriter import (
    extract_table_name,
    rewrite_for_replicated,
    split_statements,
)
from tracer.services.clickhouse.v2.attribute_catalog_codec import encode_catalog_scalar

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = (
    REPO_ROOT
    / "futureagi/tracer/services/clickhouse/v2/schema/025_span_attribute_catalog.sql"
)
FIXTURE_PATH = (
    REPO_ROOT / "fi-collector/pkg/attributecatalog/testdata/canonical_fixtures.json"
)


def _ddl_statements() -> list[str]:
    return split_statements(SCHEMA_PATH.read_text())


def test_catalog_schema_is_additive_and_independent_of_spans() -> None:
    statements = _ddl_statements()
    assert [extract_table_name(stmt) for stmt in statements] == [
        "span_attribute_key_catalog",
        "span_attribute_value_catalog",
        "span_attribute_catalog_coverage",
    ]
    executable = "\n".join(statements).lower()
    assert "alter table" not in executable
    assert "materialized view" not in executable
    assert re.search(r"\bfrom\s+spans\b", executable) is None
    assert "occurrence" not in executable
    assert re.search(r"\bcount\w*\s+", executable) is None


def test_catalog_schema_pins_scale_and_identity_invariants() -> None:
    statements = _ddl_statements()
    assert len(statements) == 3
    assert sum("ENGINE = AggregatingMergeTree" in stmt for stmt in statements) == 2
    assert (
        sum("ENGINE = ReplacingMergeTree(version)" in stmt for stmt in statements) == 1
    )
    assert all(
        "PARTITION BY cityHash64(project_id) % 64" in stmt for stmt in statements
    )
    assert all("catalog_epoch" in stmt.partition("ORDER BY")[2] for stmt in statements)
    assert "value_fingerprint FixedString(64)" in statements[1]
    assert "SimpleAggregateFunction(anyLast, String)" in statements[1]
    assert "ngrambf_v1" in statements[0]
    assert "ngrambf_v1" in statements[1]

    for statement in statements:
        table = extract_table_name(statement)
        rewritten = rewrite_for_replicated(
            statement,
            table_name=table,
            cluster="default",
            zk_prefix="/clickhouse/tables",
        )
        assert "Replicated" in rewritten
        assert "ON CLUSTER 'default'" in rewritten


def test_python_codec_matches_shared_golden_fixtures() -> None:
    document = json.loads(FIXTURE_PATH.read_text(), parse_float=Decimal)
    for fixture in document["fixtures"]:
        encoded = encode_catalog_scalar(fixture["value"])
        assert encoded.kind == fixture["kind"], fixture["name"]
        assert encoded.value_json == fixture["value_json"], fixture["name"]
        assert encoded.search_text == fixture["search_text"], fixture["name"]
        assert encoded.fingerprint == fixture["fingerprint"], fixture["name"]
        assert re.fullmatch(r"[0-9a-f]{64}", encoded.fingerprint)


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {},
        float("nan"),
        float("inf"),
        Decimal("1e5000"),
        Decimal("1e-5000"),
    ],
)
def test_python_codec_rejects_non_selectable_or_non_finite_values(
    value: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        encode_catalog_scalar(value)
