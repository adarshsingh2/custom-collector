package metricsreceiver

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type metricsReceiver struct {
	host         component.Host
	cancel       context.CancelFunc
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	config       *Config
	token        string
	tokenExpiry  time.Time
	tokenMutex   sync.Mutex // To protect concurrent access to token and tokenExpiry
}

type TokenResponse struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Data    struct {
		Token        string `json:"token"`
		IdToken      string `json:"idToken"`
		RefreshToken string `json:"refreshToken"`
	} `json:"data"`
}

type StorageDevice struct {
	SerialNumber string `json:"serialNumber"`
	ResourceID   string `json:"resourceId"`
	Model        string `json:"model"`
}

type ComputeDevice struct {
	ResourceID  string `json:"resourceId"`
	BiosVersion string `json:"biosVersion"`
	Model       string `json:"model"`
	Serial      string `json:"serial"`
}

type System struct {
	ResourceID     string          `json:"resourceId"`
	Name           string          `json:"name"`
	ComputeDevices []ComputeDevice `json:"computeDevices"`
	StorageDevices []StorageDevice `json:"storageDevices"`
	SerialNumber   string          `json:"serialNumber"`
	Model          string          `json:"model"`
	Zone           string          `json:"zone"`
}

type Systems struct {
	Path    string   `json:"path"`
	Message string   `json:"message"`
	Data    []System `json:"data"`
}

type EndpointFormat struct {
	URLFormat string
}

var endpointFormats = map[string]EndpointFormat{
	"storage": {
		URLFormat: "%s/monitoring/storage/%s/ports/metrics",
	},
	"compute": {
		URLFormat: "%s/monitoring/compute/%s/power",
	},
}

func (metricsRcvr *metricsReceiver) Start(ctx context.Context, host component.Host) error {
	metricsRcvr.host = host
	ctx, metricsRcvr.cancel = context.WithCancel(ctx) // Use the provided context

	// Start a goroutine for each ScrapeConfig
	for _, config := range metricsRcvr.config.ScrapeConfig {
		go func(config ScrapeConfig) {
			interval, err := time.ParseDuration(config.Interval)
			if err != nil {
				metricsRcvr.logger.Error("Failed to parse interval", zap.String("interval", config.Interval), zap.Error(err))
				return
			}

			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					metricsRcvr.logger.Info("Fetching metrics from endpoints", zap.String("device", config.Device))
					err := metricsRcvr.fetchAndConsumeMetrics(ctx, config)
					if err != nil {
						metricsRcvr.logger.Error("Failed to consume metrics", zap.Error(err))
					}
				case <-ctx.Done():
					metricsRcvr.logger.Info("Context canceled, stopping scraping for device", zap.String("device", config.Device))
					return
				}
			}
		}(config) // Pass the config to the goroutine
	}

	return nil
}

func (metricsRcvr *metricsReceiver) Shutdown(ctx context.Context) error {
	if metricsRcvr.cancel != nil {
		metricsRcvr.cancel()
	}
	return nil
}

func (metricsRcvr *metricsReceiver) fetchAndConsumeMetrics(ctx context.Context, config ScrapeConfig) error {
	// Step 1: Get token
	token, err := metricsRcvr.getToken()
	if err != nil {
		return fmt.Errorf("failed to get token: %w", err)
	}

	// Step 2: Get compute instances
	systems, err := metricsRcvr.getAllSystems(token)
	if err != nil {
		return fmt.Errorf("failed to get compute instances: %w", err)
	}

	// Step 3: Fetch metrics for each system
	var wg sync.WaitGroup

	for _, system := range systems.Data {
		switch config.Device {
		case "compute":
			for _, device := range system.ComputeDevices {
				wg.Add(1)
				system := system
				go func(device ComputeDevice) {
					defer wg.Done()
					err := metricsRcvr.fetchAndProcessMetrics(ctx, token, "compute", device.ResourceID, system, device, config)
					if err != nil {
						metricsRcvr.logger.Error("Failed to fetch metrics for compute instance", zap.String("instance_id", device.ResourceID), zap.Error(err))
					}
				}(device)
			}

		case "storage":
			for _, device := range system.StorageDevices {
				wg.Add(1)
				system := system
				go func(device StorageDevice) {
					defer wg.Done()
					err := metricsRcvr.fetchAndProcessMetrics(ctx, token, "storage", device.ResourceID, system, device, config)
					if err != nil {
						metricsRcvr.logger.Error("Failed to fetch metrics for storage instance", zap.String("instance_id", device.ResourceID), zap.Error(err))
					}
				}(device)
			}

		default:
			return fmt.Errorf("unknown device type: %s", config.Device)
		}
	}

	// Wait for all goroutines to complete
	wg.Wait()

	return nil
}

func (metricsRcvr *metricsReceiver) getHttpClient() *http.Client {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Disable SSL verification
		},
	}
	client := &http.Client{Transport: tr}
	return client
}

func (metricsRcvr *metricsReceiver) getToken() (string, error) {
	metricsRcvr.tokenMutex.Lock()
	defer metricsRcvr.tokenMutex.Unlock()

	// Check if the current token is still valid
	if metricsRcvr.token != "" && time.Now().Before(metricsRcvr.tokenExpiry) {
		return metricsRcvr.token, nil
	}

	reqBody := `{"username": "ucpadmin", "password": "MYPassw0rd@123"}`
	req, err := http.NewRequest("POST", metricsRcvr.config.AuthEndpoint, strings.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := metricsRcvr.getHttpClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var tokenResponse TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResponse); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	// Parse the JWT token to get the expiration time
	tokenParts := strings.Split(tokenResponse.Data.Token, ".")
	if len(tokenParts) != 3 {
		return "", fmt.Errorf("invalid token format")
	}

	// Decode the token payload
	payload, err := base64.RawURLEncoding.DecodeString(tokenParts[1])
	if err != nil {
		return "", fmt.Errorf("failed to decode token payload: %w", err)
	}

	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("failed to parse token claims: %w", err)
	}

	// Set the new token and its expiry time
	metricsRcvr.token = tokenResponse.Data.Token
	metricsRcvr.tokenExpiry = time.Unix(claims.Exp, 0)

	return metricsRcvr.token, nil
}

func (metricsRcvr *metricsReceiver) getAllSystems(token string) (*Systems, error) {
	req, err := http.NewRequest("GET", metricsRcvr.config.ComputeEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("accept", "application/json")

	resp, err := metricsRcvr.getHttpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get compute instances: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var systems Systems
	if err := json.NewDecoder(resp.Body).Decode(&systems); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Return the entire compute instances response
	return &systems, nil
}

func (metricsRcvr *metricsReceiver) fetchAndProcessMetrics(ctx context.Context, token string, endpoint string, resourceID string, system System, device interface{}, config ScrapeConfig) error {

	// Skip specific resourceID for "storage" endpoint
	if endpoint == "storage" && resourceID == "storage-2da44b3187ef43f30627344ee47d0741" {
		metricsRcvr.logger.Info("Skipping metrics fetch for resourceID", zap.String("resourceID", resourceID))
		return nil
	}

	format, exists := endpointFormats[endpoint]
	if !exists {
		return fmt.Errorf("unknown endpoint format: %s", endpoint)
	}

	metricsURL := fmt.Sprintf(format.URLFormat, metricsRcvr.config.MetricsBaseURL, resourceID)

	// Parse the timeout duration from ScrapeConfig
	timeout, err := time.ParseDuration(config.Timeout)
	if err != nil {
		return fmt.Errorf("invalid timeout value in ScrapeConfig: %w", err)
	}

	// Create a new context with a timeout for the HTTP request
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, "GET", metricsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create metrics request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	// Log the start time
	startTime := time.Now()
	metricsRcvr.logger.Info("Fetching metrics", zap.String("url", metricsURL), zap.String("resourceID", resourceID))

	// Make the HTTP request using the custom HTTP client with a timeout
	resp, err := metricsRcvr.getHttpClient().Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch metrics: %w", err)
	}
	defer resp.Body.Close()

	// Log the end time and calculate the duration
	duration := time.Since(startTime)
	metricsRcvr.logger.Info("Metrics response received",
		zap.String("url", metricsURL),
		zap.String("resourceID", resourceID),
		zap.Int("status_code", resp.StatusCode),
		zap.Duration("duration", duration),
	)

	// Parse the JSON response into a map
	var response map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Handle different types of "data" field
	switch data := response["data"].(type) {
	case map[string]interface{}:
		// If "data" is a map, process it directly
		return metricsRcvr.parseAndProcessMetrics(ctx, data, resourceID, system, device, config)

	case []interface{}:
		// If "data" is an array, iterate and process each element
		for _, item := range data {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				return fmt.Errorf("unexpected item structure in data array")
			}
			if err := metricsRcvr.parseAndProcessMetrics(ctx, itemMap, resourceID, system, device, config); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("unexpected response structure")
	}
}

func (metricsRcvr *metricsReceiver) parseAndProcessMetrics(ctx context.Context, data map[string]interface{}, instanceID string, system System, device interface{}, config ScrapeConfig) error {
	// Create an empty pmetric.Metrics object
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "myservice")
	rm.Resource().Attributes().PutStr("instance_id", instanceID)
	ilms := rm.ScopeMetrics().AppendEmpty()
	ilms.Scope().SetName("myscope")

	// Create a map to store labels and timestamp
	labels := pcommon.NewMap()
	labels.PutStr("instance_id", instanceID)
	labels.PutStr("system_resource_id", system.ResourceID)
	labels.PutStr("system_name", system.Name)
	labels.PutStr("system_serial_number", system.SerialNumber)
	labels.PutStr("system_model", system.Model)
	labels.PutStr("system_zone", system.Zone)

	var metricTimestamp pcommon.Timestamp
	hasTimestamp := false

	// Add specific labels based on the type of device (ComputeDevice or StorageDevice)
	switch v := device.(type) {
	case ComputeDevice:
		labels.PutStr("device_resource_id", v.ResourceID)
		labels.PutStr("device_bios_version", v.BiosVersion)
		labels.PutStr("device_model", v.Model)
		labels.PutStr("device_serial", v.Serial)
	case StorageDevice:
		labels.PutStr("device_resource_id", v.ResourceID)
		labels.PutStr("device_serial_number", v.SerialNumber)
		labels.PutStr("device_model", v.Model)
	}

	// First pass: Collect all labels and timestamps
	for key, value := range data {
		if contains(config.TimestampKeys, key) {
			// If the key is defined as a timestamp in the config
			if timestamp, err := time.Parse(time.RFC3339, fmt.Sprintf("%v", value)); err == nil {
				metricTimestamp = pcommon.NewTimestampFromTime(timestamp)
				hasTimestamp = true
			} else {
				metricsRcvr.logger.Warn("Invalid timestamp format", zap.String("key", key), zap.String("value", fmt.Sprintf("%v", value)))
			}
		} else if !contains(config.MetricKeys, key) {
			// If the key is defined as a label in the config
			labels.PutStr(key, fmt.Sprintf("%v", value))
		}
	}

	// Second pass: Create metrics and apply labels and timestamp
	for key, value := range data {
		if contains(config.MetricKeys, key) {
			// If the key is defined as a metric in the config
			if num, err := strconv.ParseFloat(fmt.Sprintf("%v", value), 64); err == nil {
				metric := ilms.Metrics().AppendEmpty()
				metric.SetName(key)
				metric.SetUnit("1") // Set appropriate unit
				metric.SetEmptyGauge()
				dp := metric.Gauge().DataPoints().AppendEmpty()

				// Use the collected timestamp or current time if none was found
				if hasTimestamp {
					dp.SetTimestamp(metricTimestamp)
				} else {
					dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				}

				// Set labels for the metric
				labels.CopyTo(dp.Attributes())
				dp.SetDoubleValue(num)
			} else {
				metricsRcvr.logger.Warn("Skipping non-numeric metric value", zap.String("key", key), zap.String("value", fmt.Sprintf("%v", value)))
			}
		}
	}

	// Send the converted metrics to the next consumer
	return metricsRcvr.nextConsumer.ConsumeMetrics(ctx, metrics)
}

// Helper function to check if a string is in a slice
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
