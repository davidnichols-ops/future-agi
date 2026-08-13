-- =============================================================================
-- 025 — ingestion-fed span-attribute catalog (storage contract only)
-- =============================================================================
--
-- This migration creates the independent lookup storage used by the later
-- PostHog-style attribute-discovery path. It is deliberately SCHEMA ONLY:
-- no ALTER of `spans`, no insert-time materialized view, no backfill, and no
-- reader/writer activation. A later, feature-gated change will dual-write
-- bounded catalog batches from ingestion and populate history explicitly.
--
-- SCALE / SAFETY CONTRACT
--   * Three independent tables keep key discovery, selectable values, and
--     coverage state independently operable.
--   * Project-hash partitioning has a fixed 64-bucket ceiling. It avoids both
--     a partition per tenant and time-window fan-out for retained lookups.
--   * `catalog_epoch` is part of every sorting key so a rebuild/cutover never
--     mixes generations.
--   * There are intentionally NO occurrence counters. Replayed ingestion can
--     update first/last-seen bounds without inflating a user-visible count.
--   * Value fingerprints are lowercase SHA-256 hex (`FixedString(64)`), i.e.
--     a semantic 256-bit fingerprint transported safely over JSONEachRow.
--     The shared Python/Go codec and golden fixtures pin the byte contract.
--
-- VALUE SHAPE CONTRACT
--   * string / number / boolean attributes may emit one selectable value.
--   * arrays may emit their scalar members independently; nested arrays,
--     objects, and null members emit no value row.
--   * map / json attributes are key-only and emit no value row.
-- These rules prevent row-expanding work inside ClickHouse. Any expansion is
-- bounded in the application/collector batch before insertion.
-- =============================================================================

CREATE TABLE IF NOT EXISTS span_attribute_key_catalog
(
    project_id      UUID,
    attribute_key   String,
    key_folded      String,
    attribute_type  Enum8(
        'string' = 1,
        'number' = 2,
        'boolean' = 3,
        'array' = 4,
        'map' = 5,
        'json' = 6
    ),
    first_seen      SimpleAggregateFunction(min, DateTime64(6, 'UTC')),
    last_seen       SimpleAggregateFunction(max, DateTime64(6, 'UTC')),
    catalog_epoch   UInt16,

    INDEX idx_catalog_key_ngram key_folded
        TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1
)
ENGINE = AggregatingMergeTree
PARTITION BY cityHash64(project_id) % 64
ORDER BY (project_id, catalog_epoch, key_folded, attribute_key, attribute_type)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS span_attribute_value_catalog
(
    project_id        UUID,
    attribute_key     String,
    attribute_type    Enum8(
        'string' = 1,
        'number' = 2,
        'boolean' = 3,
        'array' = 4,
        'map' = 5,
        'json' = 6
    ),
    value_fingerprint FixedString(64),
    value_json        SimpleAggregateFunction(anyLast, String),
    value_search_text SimpleAggregateFunction(anyLast, String),
    first_seen        SimpleAggregateFunction(min, DateTime64(6, 'UTC')),
    last_seen         SimpleAggregateFunction(max, DateTime64(6, 'UTC')),
    catalog_epoch     UInt16,

    INDEX idx_catalog_value_ngram lower(value_search_text)
        TYPE ngrambf_v1(3, 32768, 3, 0) GRANULARITY 1
)
ENGINE = AggregatingMergeTree
PARTITION BY cityHash64(project_id) % 64
ORDER BY
(
    project_id,
    catalog_epoch,
    attribute_key,
    attribute_type,
    value_fingerprint
)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS span_attribute_catalog_coverage
(
    project_id       UUID,
    catalog_source   Enum8('writer' = 1, 'backfill' = 2),
    catalog_epoch    UInt16,
    watermark        Nullable(DateTime64(6, 'UTC')),
    coverage_status  Enum8(
        'pending' = 1,
        'running' = 2,
        'complete' = 3,
        'gap' = 4,
        'failed' = 5
    ),
    gap_start        Nullable(DateTime64(6, 'UTC')),
    gap_end          Nullable(DateTime64(6, 'UTC')),
    gap_reason       String DEFAULT '',
    updated_at       DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC'),
    version          DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY cityHash64(project_id) % 64
ORDER BY (project_id, catalog_source, catalog_epoch)
SETTINGS index_granularity = 8192;
