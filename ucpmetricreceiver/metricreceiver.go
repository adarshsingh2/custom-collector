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

type ComputeDevice struct {
	ResourceID  string `json:"resourceId"`
	BiosVersion string `json:"biosVersion"`
	Model       string `json:"model"`
	Serial      string `json:"serial"`
	BmcAddress  string `json:"bmcAddress"`
}

type System struct {
	ResourceID     string          `json:"resourceId"`
	Name           string          `json:"name"`
	ComputeDevices []ComputeDevice `json:"computeDevices"`
	StorageDevices []StorageDevice `json:"storageDevices"`
	SerialNumber   string          `json:"serialNumber"`
	Model          string          `json:"model"`
	Zone           string          `json:"zone"`
	GatewayAddress string          `json:"gatewayAddress"`
}

type Systems struct {
	Path    string   `json:"path"`
	Message string   `json:"message"`
	Data    []System `json:"data"`
}

type StorageDevice struct {
	SerialNumber   string `json:"serialNumber"`
	ResourceID     string `json:"resourceId"`
	Model          string `json:"model"`
	GatewayAddress string `json:"gatewayAddress"`
	ManagementIP   string `json:"address"`
}

// Add common headers to a request.
func (metricsRcvr *metricsReceiver) addCommonHeaders(req *http.Request, device StorageDevice) {
	req.Header.Set("X-Management-IPs", device.ManagementIP)
	req.Header.Set("X-Subsystem-User", "ms_vmware")
	req.Header.Set("X-Subsystem-Password", "Hitachi1")
	req.Header.Set("X-Storage-Id", device.SerialNumber)
}

// Aggregate multiple errors into a single error message.
func (metricsRcvr *metricsReceiver) aggregateErrors(aggregatedErrors []error) error {
	var errorMsg strings.Builder
	errorMsg.WriteString("Errors occurred during metrics fetch: \n")
	for _, err := range aggregatedErrors {
		errorMsg.WriteString(fmt.Sprintf("- %s\n", err.Error()))
	}
	return fmt.Errorf(errorMsg.String())
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
					metricsRcvr.logger.Info("Fetching metrics from endpoints", zap.String("device", config.DeviceType))
					err := metricsRcvr.fetchAndConsumeMetrics(ctx, config)
					if err != nil {
						metricsRcvr.logger.Error("Failed to consume metrics", zap.Error(err))
					}
				case <-ctx.Done():
					metricsRcvr.logger.Info("Context canceled, stopping scraping for device", zap.String("device", config.DeviceType))
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

func (metricsRcvr *metricsReceiver) fetchSystemData(token string) (*Systems, error) {
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

func (metricsRcvr *metricsReceiver) fetchAndConsumeMetrics(ctx context.Context, config ScrapeConfig) error {
	metricsRcvr.logger.Info("Starting to fetch and consume metrics", zap.String("deviceType", config.DeviceType))

	// Step 1: Get token
	tokenStartTime := time.Now()
	token, err := metricsRcvr.getToken()
	if err != nil {
		metricsRcvr.logger.Error("Failed to get token", zap.Error(err), zap.Duration("timeTaken", time.Since(tokenStartTime)))
		return fmt.Errorf("failed to get token: %w", err)
	}
	metricsRcvr.logger.Info("Successfully retrieved token", zap.Duration("timeTaken", time.Since(tokenStartTime)))

	// Step 2: Get compute instances
	systemStartTime := time.Now()
	systems, err := metricsRcvr.fetchSystemData(token)
	if err != nil {
		metricsRcvr.logger.Error("Failed to get compute instances", zap.Error(err), zap.Duration("timeTaken", time.Since(systemStartTime)))
		return fmt.Errorf("failed to get compute instances: %w", err)
	}
	metricsRcvr.logger.Info("Successfully retrieved compute instances", zap.Int("systemsCount", len(systems.Data)), zap.Duration("timeTaken", time.Since(systemStartTime)))

	// Step 3: Fetch metrics for each system
	var wg sync.WaitGroup
	for _, system := range systems.Data {
		metricsRcvr.logger.Info("Processing system", zap.String("systemId", system.Name))
		switch config.DeviceType {
		case "compute":
			metricsRcvr.logger.Info("Fetching metrics for compute devices", zap.Int("deviceCount", len(system.ComputeDevices)))
			parser := ComputeMetricParser{metricsRcvr: metricsRcvr}
			for _, device := range system.ComputeDevices {
				wg.Add(1)
				go func(device ComputeDevice) {
					defer wg.Done()
					deviceStartTime := time.Now()
					metricsRcvr.logger.Debug("Fetching compute metrics", zap.String("instance_id", device.ResourceID))
					err := parser.fetchComputeMetrics(ctx, token, device.ResourceID, system, device, config)
					if err != nil {
						metricsRcvr.logger.Error("Failed to fetch metrics for compute instance", zap.String("instance_id", device.ResourceID), zap.Error(err), zap.Duration("timeTaken", time.Since(deviceStartTime)))
					} else {
						metricsRcvr.logger.Info("Successfully fetched compute metrics", zap.String("instance_id", device.ResourceID), zap.Duration("timeTaken", time.Since(deviceStartTime)))
					}
				}(device)
			}

		case "storage":
			metricsRcvr.logger.Info("Fetching metrics for storage devices", zap.Int("deviceCount", len(system.StorageDevices)))
			for _, device := range system.StorageDevices {
				wg.Add(1)
				go func(device StorageDevice) {
					defer wg.Done()
					deviceStartTime := time.Now()
					metricsRcvr.logger.Debug("Fetching storage metrics", zap.String("instance_id", device.ResourceID))
					err := metricsRcvr.fetchStorageMetrics(ctx, token, device.ResourceID, system, device, config)
					if err != nil {
						metricsRcvr.logger.Error("Failed to fetch metrics for storage instance", zap.String("instance_id", device.ResourceID), zap.Error(err), zap.Duration("timeTaken", time.Since(deviceStartTime)))
					} else {
						metricsRcvr.logger.Info("Successfully fetched storage metrics", zap.String("instance_id", device.ResourceID), zap.Duration("timeTaken", time.Since(deviceStartTime)))
					}
				}(device)
			}

		default:
			metricsRcvr.logger.Error("Unknown device type encountered", zap.String("deviceType", config.DeviceType))
			return fmt.Errorf("unknown device type: %s", config.DeviceType)
		}
	}

	// Wait for all goroutines to complete
	metricsRcvr.logger.Debug("Waiting for all metrics fetching routines to complete")
	wgStartTime := time.Now()
	wg.Wait()
	metricsRcvr.logger.Info("All metrics fetching routines completed", zap.Duration("timeTaken", time.Since(wgStartTime)))

	metricsRcvr.logger.Info("Completed fetching and consuming metrics", zap.String("deviceType", config.DeviceType), zap.Duration("totalTimeTaken", time.Since(tokenStartTime)))
	return nil
}

func (metricsRcvr *metricsReceiver) processParsedMetrics(ctx context.Context, data map[string]interface{}, instanceID string, system System, device interface{}, config ScrapeConfig) error {
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

