package partialtracesamplerprocessor

import (
	"fmt"
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
	Rules                     []RuleConfig `mapstructure:"rules"`
}

// Validate checks if the processor configuration is valid.
func (c *Config) Validate() error {
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
