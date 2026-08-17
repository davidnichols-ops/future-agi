package propertycatalog

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// RuntimeMode is intentionally narrower than the pre-release attribute
// catalog switch. The unified hot path is Kafka-only; direct ClickHouse
// delivery belongs to the bounded reconciler and is never available here.
type RuntimeMode string

const (
	RuntimeDisabled RuntimeMode = "disabled"
	RuntimeKafka    RuntimeMode = "kafka"

	DevelopmentEnvironment = "development"
	// DevelopmentAcknowledgement is deliberately long and version-specific so
	// copying only FI_PROPERTY_CATALOG_MODE cannot activate the writer.
	DevelopmentAcknowledgement = "TH7247_UNIFIED_PROPERTY_CATALOG_V1_DEV_ONLY"
)

type KafkaRuntimeConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

// RuntimeConfig owns only collector-side hot attribute production. It has no
// ClickHouse credentials, consumer group, or generic destination table.
type RuntimeConfig struct {
	Mode                       RuntimeMode        `yaml:"mode"`
	Environment                string             `yaml:"environment"`
	DevelopmentAcknowledgement string             `yaml:"development_acknowledgement"`
	CatalogEpoch               uint16             `yaml:"catalog_epoch"`
	ProjectionVersion          uint16             `yaml:"projection_version"`
	ProducerStreamID           string             `yaml:"producer_stream_id"`
	WorkspaceAllowlist         []string           `yaml:"workspace_allowlist"`
	RevisionFenceFile          string             `yaml:"revision_fence_file"`
	SpoolDirectory             string             `yaml:"spool_directory"`
	ReplayInterval             time.Duration      `yaml:"replay_interval"`
	QueueDepth                 int                `yaml:"queue_depth"`
	MaxSpansPerBatch           int                `yaml:"max_spans_per_batch"`
	MaxKeysPerSpan             int                `yaml:"max_keys_per_span"`
	MaxArrayMembersPerSpan     int                `yaml:"max_array_members_per_span"`
	MaxEncodedBytesPerSpan     int                `yaml:"max_encoded_bytes_per_span"`
	MaxChunkRows               int                `yaml:"max_chunk_rows"`
	MaxChunkBytes              int                `yaml:"max_chunk_bytes"`
	MaxSpoolFiles              int                `yaml:"max_spool_files"`
	MaxSpoolBytes              int64              `yaml:"max_spool_bytes"`
	Kafka                      KafkaRuntimeConfig `yaml:"kafka"`
}

func (c RuntimeConfig) normalizedMode() RuntimeMode {
	if c.Mode == "" {
		return RuntimeDisabled
	}
	return RuntimeMode(strings.ToLower(strings.TrimSpace(string(c.Mode))))
}

func (c RuntimeConfig) WithDefaults() RuntimeConfig {
	if c.ReplayInterval == 0 {
		c.ReplayInterval = time.Second
	}
	if c.QueueDepth == 0 {
		c.QueueDepth = 64
	}
	if c.MaxSpansPerBatch == 0 {
		c.MaxSpansPerBatch = 20_000
	}
	if c.MaxKeysPerSpan == 0 {
		c.MaxKeysPerSpan = 128
	}
	if c.MaxArrayMembersPerSpan == 0 {
		c.MaxArrayMembersPerSpan = 256
	}
	if c.MaxEncodedBytesPerSpan == 0 {
		c.MaxEncodedBytesPerSpan = 64 << 10
	}
	if c.MaxChunkRows == 0 {
		c.MaxChunkRows = 2_000
	}
	if c.MaxChunkBytes == 0 {
		c.MaxChunkBytes = 256 << 10
	}
	if c.MaxSpoolFiles == 0 {
		c.MaxSpoolFiles = 10_000
	}
	if c.MaxSpoolBytes == 0 {
		c.MaxSpoolBytes = 512 << 20
	}
	return c
}

func (c RuntimeConfig) Validate() error {
	mode := c.normalizedMode()
	switch mode {
	case RuntimeDisabled:
		return nil
	case RuntimeKafka:
	default:
		return fmt.Errorf("propertycatalog: invalid runtime mode %q", c.Mode)
	}
	c = c.WithDefaults()
	if c.Environment != DevelopmentEnvironment ||
		c.DevelopmentAcknowledgement != DevelopmentAcknowledgement {
		return errors.New("propertycatalog: unified ingestion is development-only and requires the exact acknowledgement")
	}
	if c.CatalogEpoch == 0 || c.ProjectionVersion == 0 {
		return errors.New("propertycatalog: enabled runtime requires positive epoch and projection version")
	}
	if err := validateCanonicalUUID("producer stream", c.ProducerStreamID); err != nil {
		return err
	}
	if c.SpoolDirectory == "" || !filepath.IsAbs(c.SpoolDirectory) {
		return errors.New("propertycatalog: enabled runtime requires an absolute dedicated spool directory")
	}
	if c.RevisionFenceFile == "" || !filepath.IsAbs(c.RevisionFenceFile) ||
		filepath.Clean(c.RevisionFenceFile) == filepath.Clean(c.SpoolDirectory) {
		return errors.New("propertycatalog: enabled runtime requires an absolute revision fence file")
	}
	if c.ReplayInterval <= 0 || c.ReplayInterval > 30*time.Second {
		return errors.New("propertycatalog: replay interval must be in (0,30s]")
	}
	if len(c.WorkspaceAllowlist) == 0 || len(c.WorkspaceAllowlist) > 256 {
		return errors.New("propertycatalog: enabled runtime requires 1..256 allowlisted workspaces")
	}
	if !slices.IsSorted(c.WorkspaceAllowlist) {
		return errors.New("propertycatalog: workspace allowlist must be sorted")
	}
	for index, workspaceID := range c.WorkspaceAllowlist {
		if err := validateCanonicalUUID(fmt.Sprintf("workspace allowlist %d", index), workspaceID); err != nil {
			return err
		}
		if index > 0 && workspaceID == c.WorkspaceAllowlist[index-1] {
			return errors.New("propertycatalog: workspace allowlist contains a duplicate")
		}
	}
	if c.QueueDepth < 1 || c.QueueDepth > 1_024 ||
		c.MaxSpansPerBatch < 1 || c.MaxSpansPerBatch > 100_000 ||
		c.MaxKeysPerSpan < 1 || c.MaxKeysPerSpan > 4_096 ||
		c.MaxArrayMembersPerSpan < 1 || c.MaxArrayMembersPerSpan > 16_384 ||
		c.MaxEncodedBytesPerSpan < 1 || c.MaxEncodedBytesPerSpan > MaxChunkBytes ||
		c.MaxChunkRows < 1 || c.MaxChunkRows > MaxRowsPerChunk ||
		c.MaxChunkBytes < 1 || c.MaxChunkBytes > MaxChunkBytes ||
		c.MaxSpoolFiles < 1 || c.MaxSpoolFiles > 1_000_000 || c.MaxSpoolBytes < 1 {
		return errors.New("propertycatalog: runtime queue/build/chunk/spool bounds are outside hard limits")
	}
	if len(c.Kafka.Brokers) == 0 || len(c.Kafka.Brokers) > 16 {
		return errors.New("propertycatalog: Kafka runtime requires 1..16 brokers")
	}
	for _, broker := range c.Kafka.Brokers {
		if broker == "" || strings.TrimSpace(broker) != broker || len(broker) > 255 {
			return errors.New("propertycatalog: Kafka broker is empty, padded, or too long")
		}
	}
	if err := validateTopic(c.Kafka.Topic); err != nil {
		return err
	}
	return nil
}

func (c RuntimeConfig) SelectedMode() (RuntimeMode, error) {
	if err := c.Validate(); err != nil {
		return RuntimeDisabled, err
	}
	return c.normalizedMode(), nil
}

func (c RuntimeConfig) WorkspaceAllowed(workspaceID string) bool {
	return slices.Contains(c.WorkspaceAllowlist, workspaceID)
}
