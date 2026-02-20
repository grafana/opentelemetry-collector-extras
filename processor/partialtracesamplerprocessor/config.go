package partialtracesamplerprocessor

import (
	"fmt"
)

// SamplerMode determines how sampling decisions are made.
type SamplerMode string

const (
	// HashSeed uses FNV hash of the trace ID with an optional seed.
	// This is the legacy mode compatible with the probabilistic sampler's hash_seed mode.
	HashSeed SamplerMode = "hash_seed"

	// Equalizing uses W3C consistent probability sampling (OTEP 235).
	// It applies an absolute threshold, never increasing the sampling probability
	// beyond what was already decided upstream.
	Equalizing SamplerMode = "equalizing"

	// Proportional uses W3C consistent probability sampling (OTEP 235).
	// It multiplies the incoming sampling probability by the configured ratio,
	// compounding with any upstream sampling decisions.
	Proportional SamplerMode = "proportional"
)

// RuleConfig defines a single sampling rule with an OTTL condition.
type RuleConfig struct {
	SamplingPercentage float32 `mapstructure:"sampling_percentage"`
	Condition          string  `mapstructure:"condition"`
}

// Config holds the configuration for the partialtracesampler processor.
type Config struct {
	DefaultSamplingPercentage float32      `mapstructure:"default_sampling_percentage"`
	HashSeed                  uint32       `mapstructure:"hash_seed"`
	SamplerMode               SamplerMode  `mapstructure:"sampler_mode"`
	SamplingPrecision         int          `mapstructure:"sampling_precision"`
	FailClosed                bool         `mapstructure:"fail_closed"`
	Rules                     []RuleConfig `mapstructure:"rules"`
}

// Validate checks if the processor configuration is valid.
func (c *Config) Validate() error {
	mode := c.SamplerMode
	if mode == "" {
		mode = HashSeed
	}
	switch mode {
	case HashSeed, Equalizing, Proportional:
	default:
		return fmt.Errorf("sampler_mode must be one of %q, %q, or %q, got %q", HashSeed, Equalizing, Proportional, c.SamplerMode)
	}
	if mode != HashSeed && (c.SamplingPrecision < 1 || c.SamplingPrecision > 14) {
		return fmt.Errorf("sampling_precision must be between 1 and 14, got %d", c.SamplingPrecision)
	}
	if c.DefaultSamplingPercentage < 0 || c.DefaultSamplingPercentage > 100 {
		return fmt.Errorf("default_sampling_percentage must be between 0 and 100, got %g", c.DefaultSamplingPercentage)
	}
	for i, r := range c.Rules {
		if r.SamplingPercentage < 0 || r.SamplingPercentage > 100 {
			return fmt.Errorf("rule[%d]: sampling_percentage must be between 0 and 100, got %g", i, r.SamplingPercentage)
		}
		if r.Condition == "" {
			return fmt.Errorf("rule[%d]: condition must not be empty", i)
		}
	}
	return nil
}
