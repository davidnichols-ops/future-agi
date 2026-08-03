"""
v2 ClickHouse filter compiler — targets the new CH 25.3 `spans` schema.

Strategy: SUBCLASS the legacy `ClickHouseFilterBuilder` so we inherit all
~1500 lines of frontend-filter-JSON parsing logic AND the shared canonical
filter contract (the operator/type/column-id rules pulled from
`api_contracts/filter_contract.json`). Then rewrite the COLUMN REFERENCES in
the compiled SQL output.

Why this works:
  - Filter operator/type/value contract is identical between v1 and v2. The
    only thing that changes is which CH column the SQL references.
  - Legacy column identifiers (`_peerdb_is_deleted`, `span_attr_str`, etc.)
    are unique tokens; word-boundary substitution is safe.
  - Typed-JSON access syntax (`attributes_extra.path.:Type`) replaces
    `JSONExtractString(span_attributes_raw, 'path')`; a few targeted regex
    rewrites cover the JSONExtract* calls v1 emits.

Why not refactor v1 to use overridable constants:
  - 41 column references across 1657 lines. Touching each line is high-risk
    on a hot dashboard path. The post-rewrite approach keeps v1 unchanged
    and isolates v2 risk to the rewrite + the parity-shadow harness.

Risk mitigations:
  - The parity-shadow harness (tracer/services/clickhouse/v2/shadow.py) runs
    v1 and v2 in parallel and logs diffs. Any v1 emission pattern the
    rewriter doesn't anticipate surfaces as a shadow diff long before any
    query type is flipped to v2-primary.
  - Tests in `tracer/tests/test_ch25_filter_compiler.py` cover every
    column-rewrite case + every JSONExtract* pattern v1 currently emits.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from tracer.services.clickhouse.query_builders.filters import (
    ClickHouseFilterBuilder,
    _coerce_strict_bool,
)
from tracer.services.clickhouse.v2.query_builders import columns as cols

# ─── Simple column-name renames ───────────────────────────────────────────────
# These are tokens; word-boundary regex substitutes them safely.
_COL_RENAMES: dict[str, str] = {
    "_peerdb_is_deleted": cols.IS_DELETED,
    "_peerdb_version": cols.VERSION,
    "span_attr_str": cols.ATTRS_STRING,
    "span_attr_num": cols.ATTRS_NUMBER,
    "span_attr_bool": cols.ATTRS_BOOL,
}

# Pre-compile a single regex that matches any legacy column name as a whole word.
_COL_RENAME_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _COL_RENAMES.keys()) + r")\b"
)


# Legacy CDC dict names → v2 CH-native dicts (same key/attrs, so a token rename).
# Sourced from the now-renamed v2 curated dimension tables (end_users RMT,
# trace_sessions RMT) instead of the legacy CDC landing tables.
_DICT_RENAMES: dict[str, str] = {
    "enduser_dict": "end_users_dict",
    "trace_session_dict": "trace_sessions_dict",
}
_DICT_RENAME_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _DICT_RENAMES.keys()) + r")\b"
)


# Eval filters are polymorphic: even when the outer spans query targets CH25,
# ``CH25_EVAL_LOGGER_TABLE`` may still select the legacy PeerDB table. The
# whole-fragment v2 rewrite must continue to migrate spans references, but it
# must not rename that table's physical version/tombstone columns. Protect only
# the dedicated eval aliases emitted by the base filter compiler, rewrite the
# rest of the fragment, then restore the legacy identifiers.
_LEGACY_EVAL_COLUMN_MARKERS: dict[str, str] = {
    "eval_scan._peerdb_version": "eval_scan.__eval_legacy_version__",
    "eval_scan._peerdb_is_deleted": "eval_scan.__eval_legacy_cdc_deleted__",
    "latest_eval._peerdb_is_deleted": "latest_eval.__eval_legacy_cdc_deleted__",
    "raw_eval_logger._peerdb_is_deleted": (
        "raw_eval_logger.__eval_legacy_cdc_deleted__"
    ),
}

_RAW_EVAL_LEGACY_COLUMN_MARKERS = {
    "raw_eval_logger._peerdb_is_deleted": (
        "raw_eval_logger.__eval_legacy_cdc_deleted__"
    ),
}


# ─── JSON-overflow access rewrites ────────────────────────────────────────────
# Schema 013 stores attributes_extra as String JSON. Preserve JSONExtract*/
# JSONHas and replace only the legacy first argument, including variadic paths.
_ATTRIBUTES_EXTRA_JSON_FUNCTION_PATTERN = re.compile(
    r"\b(?P<function>JSONExtract(?:String|Float|U?Int|Bool|ArrayRaw|Raw)|JSONHas|JSONType)"
    r"(?P<open>\s*\(\s*)span_attributes_raw(?P<comma>\s*,)"
)

# Resource attributes and metadata remain typed JSON.
_JSON_EXTRACT_PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (
        re.compile(
            r"JSONExtractString\(\s*resource_attributes_raw\s*,\s*'([^']+)'\s*\)"
        ),
        cols.RESOURCE_ATTRS,
        "String",
    ),
    (
        re.compile(r"JSONExtractString\(\s*metadata_map\s*,\s*'([^']+)'\s*\)"),
        cols.METADATA_JSON,
        "String",
    ),
]

# `JSONHas(span_attributes_raw, 'path')` → `(attributes_extra.path.:String IS NOT NULL)`
_JSON_HAS_PATTERN = re.compile(
    r"JSONHas\(\s*(resource_attributes_raw|metadata_map)\s*,\s*'([^']+)'\s*\)"
)
_JSON_HAS_TARGET = {
    "resource_attributes_raw": (cols.RESOURCE_ATTRS, "String"),
    "metadata_map": (cols.METADATA_JSON, "String"),
}

# Map from legacy bare JSON columns to v2 columns. attributes_extra is already
# String JSON; resource_attrs and metadata remain typed JSON and are stringified
# only when callers require the legacy textual row shape.
_BARE_JSON_REWRITES = {
    "span_attributes_raw": cols.ATTRIBUTES_EXTRA,
    "metadata_map": cols.METADATA_JSON,
    "resource_attributes_raw": cols.RESOURCE_ATTRS,
}
_STRING_JSON_LEGACY_COLUMNS = frozenset({"span_attributes_raw"})


def _json_text_expression(legacy_col: str, v2_col: str) -> str:
    if legacy_col in _STRING_JSON_LEGACY_COLUMNS:
        return v2_col
    return f"toJSONString({v2_col})"


# WHERE emptiness checks v1 emits: `<legacy_col> != '{}'`, `!= ''`, `= '{}'`, `= ''`.
# Pattern allows single or doubled `{}` (the `{{}}` form appears when the SQL
# was built via `f.format(...)` — the double-brace escapes inside an f-string).
_WHERE_EMPTY_PATTERN = re.compile(
    r"\b(span_attributes_raw|resource_attributes_raw|metadata_map)\b"
    r"\s*(!=|=)\s*'(\{?\}?|\{\{?\}?\}?)'"
)

# Bare SELECT-list / projection reference. The negative lookahead skips matches
# that are immediately followed by `[` (Map subscript — but the legacy Map
# columns are span_attr_*, never these) or `(` (function call — already
# consumed by the JSONExtract patterns above), and negative lookbehind skips
# matches inside identifiers (preceded by alphanumeric or underscore).
_BARE_REF_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_'])"
    r"\b(span_attributes_raw|resource_attributes_raw|metadata_map)\b"
    r"(?![\[\(A-Za-z0-9_])"
)


# ─── v2 attribute-type meta (same shape as v1 module-level constant, retargeted) ─
_SPAN_ATTR_TYPE_META_V2: dict[str, tuple[str, Callable[[Any], Any]]] = {
    "text": (cols.ATTRS_STRING, lambda v: v if isinstance(v, str) else str(v)),
    "number": (cols.ATTRS_NUMBER, lambda v: float(v)),
    "boolean": (cols.ATTRS_BOOL, _coerce_strict_bool),
}


_V2_REQUIRED_SETTINGS = (
    # Correctness boundary for ReplacingMergeTree reads.  A skip index on a
    # column outside the sorting key may hide the newest physical version from
    # FINAL, allowing an older version to survive the merge.  List, graph, and
    # eval-filter builders accept arbitrary mutable Map/JSON/custom-attribute
    # predicates (and can include FINAL reads of dimension/score tables), so a
    # blanket opt-in is not sound.  Pin the safe ClickHouse default explicitly;
    # this also makes ordinary application reads match the server-enforced
    # read-only A/B profile, which locks this setting to zero.
    #
    # Narrow point reads over stable identity keys opt in separately in
    # ``v2.span_reader._FINAL_SKIP_INDEX_SETTINGS``.  Those queries deliberately
    # omit mutable ``is_deleted``/attribute predicates, preserving their bloom-
    # index speedup without weakening the general query-builder contract.
    "use_skip_indexes_if_final = 0",
    # Encourage projection auto-routing for dashboard aggregates. Falls
    # through to base-table read if no projection matches — zero risk.
    "optimize_use_projections = 1",
    # Streaming aggregation order: when ORDER BY matches the table's ORDER
    # BY prefix, CH can stream-aggregate without sorting. Big win on time-
    # bucketed dashboard queries.
    "optimize_aggregation_in_order = 1",
)


def _append_v2_settings(sql: str) -> str:
    """Append the v2-required settings to a SQL string.

    Idempotent: if the SQL already ends with a SETTINGS clause, merge the
    v2 settings into it (don't double-apply). If not, append a fresh one.

    Handles trailing FORMAT clause: SETTINGS must come BEFORE FORMAT.
    """
    sql_stripped = sql.rstrip().rstrip(";").rstrip()
    # Check for an existing SETTINGS clause (case-insensitive, at end before
    # any FORMAT clause). Use a simple heuristic — the v1 builders rarely
    # emit SETTINGS, so the common case is "no existing clause."
    import re as _re

    # Pull out a trailing FORMAT clause so we can re-attach it after SETTINGS.
    fmt_match = _re.search(r"\s+FORMAT\s+\w+\s*$", sql_stripped, _re.IGNORECASE)
    if fmt_match:
        format_clause = fmt_match.group(0)
        sql_stripped = sql_stripped[: fmt_match.start()].rstrip()
    else:
        format_clause = ""

    settings_clause = "SETTINGS " + ", ".join(_V2_REQUIRED_SETTINGS)
    existing = _re.search(r"\s+SETTINGS\s+", sql_stripped, _re.IGNORECASE)
    if existing:
        # Merge — append our settings to the existing clause (later wins on
        # duplicate keys, which is what we want).
        sql_stripped = sql_stripped + ", " + ", ".join(_V2_REQUIRED_SETTINGS)
    else:
        sql_stripped = sql_stripped + "\n" + settings_clause

    return sql_stripped + format_clause


def rewrite_v1_sql_to_v2(sql: str) -> str:
    """Translate a v1-compiled SQL string to v2 column references.

    Public so tests can pin every rewrite case directly without going through
    the full filter compiler.

    Order matters:
      1. String JSON access keeps JSONExtract*/JSONHas and retargets its first
         argument to attributes_extra.
      2. Typed resource/metadata JSON access becomes typed path syntax.
      3. WHERE emptiness predicates — `WHERE legacy_col != '{}'` →
         length-based check on the JSON string representation.
      4. Bare SELECT-list refs — `SELECT … legacy_col …` →
         `SELECT … <json text> AS legacy_col …`. Preserves the
         downstream Python `row["legacy_col"]` shape (still a JSON string).
      5. Naked simple renames — `_peerdb_is_deleted` → `is_deleted`, etc.
         Word-boundary substitution; runs last.
      6. Append v2-required settings (use_skip_indexes_if_final etc).
    """
    # A legacy raw eval table can be selected while the surrounding spans SQL
    # uses the v2 schema.  The dedicated alias is emitted only for that legacy
    # physical table, so preserve its CDC tombstone column through the global
    # token rewrite.  A v2 eval table emits ``raw_eval_logger.is_deleted`` and
    # does not need a marker.
    for source, marker in _RAW_EVAL_LEGACY_COLUMN_MARKERS.items():
        sql = sql.replace(source, marker)

    # 1. String JSON access. Replace only the first argument so nested paths
    # and escaped/unicode literals remain unchanged.
    sql = _ATTRIBUTES_EXTRA_JSON_FUNCTION_PATTERN.sub(
        lambda match: (
            f"{match.group('function')}{match.group('open')}"
            f"{cols.ATTRIBUTES_EXTRA}{match.group('comma')}"
        ),
        sql,
    )

    # 2. Typed resource/metadata JSON path access.
    for pat, target_col, ch_type in _JSON_EXTRACT_PATTERNS:
        sql = pat.sub(
            lambda m, c=target_col, t=ch_type: cols.json_path(c, m.group(1), t),
            sql,
        )

    # 2. JSON has
    def _has_repl(m):
        col, ch_type = _JSON_HAS_TARGET[m.group(1)]
        return f"({cols.json_path(col, m.group(2), ch_type)} IS NOT NULL)"

    sql = _JSON_HAS_PATTERN.sub(_has_repl, sql)

    # 3. WHERE emptiness predicates
    def _empty_repl(m):
        legacy_col = m.group(1)
        op = m.group(2)
        literal = m.group(3)
        v2_col = _BARE_JSON_REWRITES[legacy_col]
        wrapped = _json_text_expression(legacy_col, v2_col)
        # `'{}'` or `'{{}}'` mean "empty object literal" → 2 chars (or 4 if
        # the double-brace was a Python format-string escape, which CH never
        # sees — by the time SQL reaches us, the braces are concrete).
        is_empty_obj = literal in ("{}", "{{}}", "{")
        if op == "!=" and is_empty_obj:
            return f"length({wrapped}) > 2"
        if op == "!=" and literal == "":
            return f"length({wrapped}) > 0"
        if op == "=" and is_empty_obj:
            return f"length({wrapped}) <= 2"
        if op == "=" and literal == "":
            return f"length({wrapped}) = 0"
        # Fall back — wrap with toJSONString and keep the literal compare
        return f"{wrapped} {op} '{literal}'"

    sql = _WHERE_EMPTY_PATTERN.sub(_empty_repl, sql)

    # 4. Bare SELECT-list refs preserve the caller's JSON-string row shape.
    def _bare_repl(m):
        legacy_col = m.group(1)
        v2_col = _BARE_JSON_REWRITES[legacy_col]
        return f"{_json_text_expression(legacy_col, v2_col)} AS {legacy_col}"

    sql = _BARE_REF_PATTERN.sub(_bare_repl, sql)

    # 5. Naked simple renames (must come last so we don't accidentally rewrite
    # inside the AS aliases we just produced).
    sql = _COL_RENAME_RE.sub(lambda m: _COL_RENAMES[m.group(1)], sql)
    # 5b. Legacy CDC dictionary names → v2 CH-native dictionary names.
    sql = _DICT_RENAME_RE.sub(lambda m: _DICT_RENAMES[m.group(1)], sql)
    for source, marker in _RAW_EVAL_LEGACY_COLUMN_MARKERS.items():
        sql = sql.replace(marker, source)
    # NOTE: this function does NOT append the v2 SETTINGS clause. The settings
    # are appended at the BUILDER boundary (v2 `build()`/`build_count_query()` etc)
    # via `_append_v2_settings()` — see ClickHouseFilterBuilderV2.translate.
    # Keeping rewrite_v1_sql_to_v2 pure lets tests assert exact-string rewrites
    # without dragging SETTINGS into every expectation.
    return sql


def rewrite_and_apply_v2_settings(sql: str) -> str:
    """One-call helper for builder methods: pure rewrite + SETTINGS append."""
    return _append_v2_settings(rewrite_v1_sql_to_v2(sql))


class ClickHouseFilterBuilderV2(ClickHouseFilterBuilder):
    """Filter compiler for the new CH 25.3 spans schema.

    Drop-in replacement for the v1 builder:
      v1: from tracer.services.clickhouse.query_builders.filters import ClickHouseFilterBuilder
      v2: from tracer.services.clickhouse.v2.query_builders.filters import ClickHouseFilterBuilderV2

    Call sites swap one import line; everything else works.
    """

    # Expose the v2 attribute-type meta on the instance.
    SPAN_ATTR_TYPE_META = _SPAN_ATTR_TYPE_META_V2

    # End-user filter subquery reads the v2 `end_users` RMT (keyed by
    # end_user_id, soft-deleted via is_deleted) instead of the dropped legacy
    # `tracer_enduser` (id + _peerdb_is_deleted/deleted).
    _ENDUSER_DIM_TABLE = "end_users"
    _ENDUSER_DIM_ID_COL = "end_user_id"
    _ENDUSER_DIM_NOT_DELETED = "is_deleted = 0"

    @staticmethod
    def _rewrite_filter_fragment(sql: str) -> str:
        """Rewrite spans SQL without corrupting a legacy eval-table probe."""
        from tracer.services.clickhouse.eval_logger_table import eval_logger_source

        eval_table, _ = eval_logger_source()
        if eval_table.endswith("_v2"):
            return rewrite_v1_sql_to_v2(sql)

        protected = sql
        for source, marker in _LEGACY_EVAL_COLUMN_MARKERS.items():
            protected = protected.replace(source, marker)
        rewritten = rewrite_v1_sql_to_v2(protected)
        for source, marker in _LEGACY_EVAL_COLUMN_MARKERS.items():
            rewritten = rewritten.replace(marker, source)
        return rewritten

    def _span_attr_inner(
        self,
        map_column: str,
        attribute_key: str,
        exists_predicate: str,
        filter_op: str,
        normalized_value: Any,
        case_insensitive: bool = False,
    ) -> str | None:
        inner = super()._span_attr_inner(
            map_column,
            attribute_key,
            exists_predicate,
            filter_op,
            normalized_value,
            case_insensitive,
        )
        # idx_attrs_str_values is a bloom over arrayMap(x -> lower(x),
        # mapValues(attrs_string)); the lower()-wrapped equality alone can
        # never engage it, so equality/IN gain a companion predicate in the
        # index's exact expression shape. The companion is implied by the
        # real predicate (a matching row necessarily carries the lowered
        # value), so result sets are unchanged. Negations must never get
        # one (it would invert semantics) and substring ops can't use a
        # plain bloom. lower() is ASCII-only on both sides — the CH lower()
        # in the index expression and the Python .lower() on the constant
        # must stay in step or the index silently disengages.
        if (
            not inner
            or not case_insensitive
            or filter_op not in ("equals", "in")
            or map_column not in ("span_attr_str", cols.ATTRS_STRING)
        ):
            return inner
        lowered_values = f"arrayMap(x -> lower(x), mapValues({map_column}))"
        if filter_op == "equals":
            param = self._next_param("attrv")
            self._params[param] = (
                normalized_value.lower()
                if isinstance(normalized_value, str)
                else normalized_value
            )
            return f"{inner} AND has({lowered_values}, %({param})s)"
        bound = []
        for value in normalized_value:
            param = self._next_param("attrv")
            self._params[param] = value.lower() if isinstance(value, str) else value
            bound.append(f"%({param})s")
        return f"{inner} AND hasAny({lowered_values}, [{', '.join(bound)}])"

    def _span_membership_date_filter(self) -> str:
        # The CH25 spans table is partitioned by toDate(start_time) with
        # toStartOfHour(start_time) in the primary key and no index on
        # created_at, so the inherited created_at bound prunes nothing there —
        # membership subqueries scanned the project's entire span history.
        if not self.span_date_scope:
            return ""
        return (
            " AND start_time >= %(start_date)s - INTERVAL 1 DAY"
            " AND start_time < %(end_date)s + INTERVAL 1 DAY"
        )

    def translate(self, filters):  # type: ignore[override]
        # `translate` returns a WHERE fragment that gets stitched into a larger
        # SELECT statement by callers. Do NOT append SETTINGS here — that
        # happens at the full-SELECT boundary in the per-builder `build()`
        # methods (SpanListQueryBuilderV2.build, TraceListQueryBuilderV2.build,
        # etc.). Otherwise we'd end up with `WHERE ... SETTINGS ... AND ...`
        # which is a syntax error.
        sql, params = super().translate(filters)
        return self._rewrite_filter_fragment(sql), params

    def translate_sort(self, sort_params, *args, **kwargs):  # type: ignore[override]
        # Forward extra args (e.g. field_map) to the v1 implementation — callers
        # like the list builders pass field_map=SORT_FIELD_MAP.
        result = super().translate_sort(sort_params, *args, **kwargs)
        if isinstance(result, tuple):
            sql, *rest = result
            return (rewrite_v1_sql_to_v2(sql), *rest)
        return rewrite_v1_sql_to_v2(result)


__all__ = [
    "ClickHouseFilterBuilderV2",
    "rewrite_v1_sql_to_v2",
    "rewrite_and_apply_v2_settings",
]
