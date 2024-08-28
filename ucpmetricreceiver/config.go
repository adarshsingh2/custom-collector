package metricsreceiver

import (
	"fmt"
	"time"
)

// ScrapeConfig represents a single scrape configuration within the receiver's config.yaml
type ScrapeConfig struct {
	Interval      string   `mapstructure:"interval"`
	Device        string   `mapstructure:"device"`
	Timeout       string   `mapstructure:"timeout"`
	MetricKeys    []string `mapstructure:"metric_keys"`
	TimestampKeys []string `mapstructure:"timestamp_keys"`
}

// Config represents the receiver config settings within the collector's config.yaml
type Config struct {
	Endpoint        string         `mapstructure:"endpoint"`
	AuthEndpoint    string         `mapstructure:"auth_endpoint"`
	ComputeEndpoint string         `mapstructure:"compute_endpoint"`
	MetricsBaseURL  string         `mapstructure:"metrics_base_url"`
	ScrapeConfig    []ScrapeConfig `mapstructure:"scrape_config"` // Field for multiple scrape configurations
}

// Validate checks if the receiver configuration is valid
func (cfg *Config) Validate() error {
	// Validate each ScrapeConfig entry
	for _, sc := range cfg.ScrapeConfig {
		if err := sc.Validate(); err != nil {
			return fmt.Errorf("invalid scrape_config entry: %w", err)
		}
	}

	return nil
}

// Validate checks if the scrape configuration is valid
func (sc *ScrapeConfig) Validate() error {
	interval, err := time.ParseDuration(sc.Interval)
	if err != nil {
		return fmt.Errorf("invalid interval format: %w", err)
	}

	if interval.Seconds() < 10 {
		return fmt.Errorf("the interval in scrape_config has to be set to at least 10 secs (10s)")
	}

	if sc.Device == "" {
		return fmt.Errorf("device in scrape_config cannot be empty")
	}

	if sc.Timeout == "" {
		return fmt.Errorf("type in scrape_config cannot be empty")
	}

	if len(sc.MetricKeys) == 0 {
		return fmt.Errorf("metric_keys in scrape_config cannot be empty")
	}

	if len(sc.TimestampKeys) == 0 {
		return fmt.Errorf("timestamp_keys in scrape_config cannot be empty")
	}

	return nil
}
