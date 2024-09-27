package metricsreceiver

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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

type EndpointFormat struct {
	URLFormat string
}

var endpointFormats = map[string]EndpointFormat{
	"storage:port": {
		URLFormat: "https://%s:8444/v9/storage/ports/perf-stats",
	},
	"compute:power": {
		URLFormat: "https://%s/v9/compute-metrics/servers/0/power-consumption", // Power consumption
	},
	"compute:temperature": {
		URLFormat: "https://%s/v9/compute/servers/0/sensors/TEMPERATURE", // Temperature
	},
	"compute:fan": {
		URLFormat: "https://%s/v9/compute/servers/0/sensors/FAN", // Fan speed
	},
	"storage:logical-units": {
		URLFormat: "https://%s/v9/storage/logical-units/perf-stats",
	},
}

type StorageDevice struct {
	SerialNumber   string `json:"serialNumber"`
	ResourceID     string `json:"resourceId"`
	Model          string `json:"model"`
	GatewayAddress string `json:"gatewayAddress"`
	ManagementIP   string `json:"address"`
}

// Fetch storage metrics function.
func (metricsRcvr *metricsReceiver) fetchStorageMetrics(ctx context.Context, token string, resourceID string, system System, device StorageDevice, config ScrapeConfig) error {
	// Start time for performance measurement
	startTime := time.Now()
	metricsRcvr.logger.Info("Starting fetch of storage metrics",
		zap.String("storageResourceId", resourceID),
		zap.String("deviceSerialNumber", device.SerialNumber))

	// Use WaitGroup to manage concurrent fetching of metrics
	var wg sync.WaitGroup
	var mu sync.Mutex
	var aggregatedErrors []error

	// Fetch logical units metrics concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		metricsRcvr.logger.Debug("Fetching logical units metrics concurrently", zap.String("deviceSerialNumber", device.SerialNumber))
		if err := metricsRcvr.fetchLogicalUnitsMetrics(ctx, system, device, config); err != nil {
			metricsRcvr.logger.Error("Failed to fetch logical units metrics", zap.Error(err))
			mu.Lock()
			aggregatedErrors = append(aggregatedErrors, err)
			mu.Unlock()
		}
	}()

	// Step 1: List ports for the storage device
	portResponse, err := metricsRcvr.fetchPortList(ctx, token, resourceID)
	if err != nil {
		metricsRcvr.logger.Error("Failed to list ports for storage device", zap.String("storageResourceId", resourceID), zap.Error(err))
		return err
	}

	// Collect all port IDs
	var ports []string
	for _, port := range portResponse.Data {
		ports = append(ports, port.PortId)
	}
	metricsRcvr.logger.Info("Fetched list of ports", zap.Int("portCount", len(ports)), zap.String("storageResourceId", resourceID))

	// Get batch size from environment variable or default to 50 if not set or invalid
	batchSize := 50
	if batchSizeStr := os.Getenv("PORT_BATCH_SIZE"); batchSizeStr != "" {
		if parsedBatchSize, err := strconv.Atoi(batchSizeStr); err == nil && parsedBatchSize > 0 {
			batchSize = parsedBatchSize
		}
	}
	metricsRcvr.logger.Debug("Using batch size for port processing", zap.Int("batchSize", batchSize))

	// Process ports in batches
	for i := 0; i < len(ports); i += batchSize {
		end := i + batchSize
		if end > len(ports) {
			end = len(ports)
		}
		portBatch := ports[i:end]

		metricsRcvr.logger.Info("Processing port batch",
			zap.Int("batchStartIndex", i),
			zap.Int("batchEndIndex", end),
			zap.Int("batchSize", len(portBatch)))

		wg.Add(1)
		go func(portBatch []string) {
			defer wg.Done()
			metricsRcvr.logger.Debug("Fetching and processing metrics for port batch", zap.Strings("portBatch", portBatch))
			if err := metricsRcvr.fetchAndProcessPortMetrics(ctx, portBatch, system, device, &mu, &aggregatedErrors, config); err != nil {
				metricsRcvr.logger.Error("Failed to fetch and process port metrics", zap.Strings("portBatch", portBatch), zap.Error(err))
				mu.Lock()
				aggregatedErrors = append(aggregatedErrors, err)
				mu.Unlock()
			}
		}(portBatch)
	}

	// Wait for all goroutines to complete
	metricsRcvr.logger.Debug("Waiting for all goroutines to complete")
	wg.Wait()

	// Log the total time taken for fetching all metrics
	totalTime := time.Since(startTime)
	metricsRcvr.logger.Info("Completed fetch of storage metrics",
		zap.String("storageResourceId", resourceID),
		zap.Duration("totalTimeTaken", totalTime))

	// Aggregate errors (if any) and return them as a single error
	if len(aggregatedErrors) > 0 {
		metricsRcvr.logger.Error("Encountered errors during fetch", zap.Int("errorCount", len(aggregatedErrors)))
		return metricsRcvr.aggregateErrors(aggregatedErrors)
	}

	metricsRcvr.logger.Info("Successfully fetched storage metrics", zap.String("storageResourceId", resourceID))
	return nil
}

// Fetch the port list.
func (metricsRcvr *metricsReceiver) fetchPortList(ctx context.Context, token string, resourceID string) (*PortResponse, error) {
	portsURL := fmt.Sprintf("%s/porcelain/v2/storage/devices/%s/ports?refresh=false", metricsRcvr.config.MetricsBaseURL, resourceID)

	req, err := http.NewRequestWithContext(ctx, "GET", portsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create port list request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("accept", "application/json")

	resp, err := metricsRcvr.getHttpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch port list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var portResponse PortResponse
	if err := json.NewDecoder(resp.Body).Decode(&portResponse); err != nil {
		return nil, fmt.Errorf("failed to decode port list response: %w", err)
	}

	return &portResponse, nil
}

// Fetch logical units metrics concurrently.
func (metricsRcvr *metricsReceiver) fetchLogicalUnitsMetrics(ctx context.Context, system System, device StorageDevice, config ScrapeConfig) error {
	metricsURL := fmt.Sprintf(endpointFormats["storage:logical-units"].URLFormat, device.GatewayAddress)

	// Log the start of the metrics fetching process
	metricsRcvr.logger.Info("Fetching logical units metrics",
		zap.String("url", metricsURL),
		zap.String("deviceID", device.ResourceID),
	)

	req, err := http.NewRequestWithContext(ctx, "GET", metricsURL, nil)
	if err != nil {
		metricsRcvr.logger.Error("Failed to create logical units metrics request",
			zap.String("url", metricsURL),
			zap.Error(err),
		)
		return fmt.Errorf("failed to create logical units metrics request: %w", err)
	}

	// Add common headers
	metricsRcvr.addCommonHeaders(req, device)

	// Make the HTTP request
	resp, err := metricsRcvr.getHttpClient().Do(req)
	if err != nil {
		metricsRcvr.logger.Error("Failed to fetch logical units metrics",
			zap.String("url", metricsURL),
			zap.String("deviceID", device.ResourceID),
			zap.Error(err),
		)
		return fmt.Errorf("failed to fetch logical units metrics: %w", err)
	}
	defer resp.Body.Close()

	// Log response status
	if resp.StatusCode != http.StatusOK {
		metricsRcvr.logger.Error("Unexpected status code from logical units metrics URL",
			zap.String("url", metricsURL),
			zap.String("deviceID", device.ResourceID),
			zap.Int("status_code", resp.StatusCode),
		)
		return fmt.Errorf("unexpected status code %d from logical units metrics URL", resp.StatusCode)
	}

	// Decode the response
	var logicalUnitMetricsResponse map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&logicalUnitMetricsResponse); err != nil {
		metricsRcvr.logger.Error("Failed to decode logical units metrics response",
			zap.String("url", metricsURL),
			zap.String("deviceID", device.ResourceID),
			zap.Error(err),
		)
		return fmt.Errorf("failed to decode logical units metrics response: %w", err)
	}

	// Log successful decoding of metrics response
	metricsRcvr.logger.Info("Successfully decoded logical units metrics response",
		zap.String("deviceID", device.ResourceID),
	)

	// Process the logical units metrics as needed
	storageMetricsParser := &StorageMetricParser{device: device}
	processedMetricsArray, err := storageMetricsParser.parseLogicalUnitMetrics(logicalUnitMetricsResponse)
	if err != nil {
		metricsRcvr.logger.Error("Failed to parse logical units metrics",
			zap.String("deviceID", device.ResourceID),
			zap.Error(err),
		)
		return fmt.Errorf("failed to parse logical units metrics for %s: %w", device.ResourceID, err)
	}

	// Loop through the array of processed metrics and call parseAndProcessMetrics for each
	for _, processedMetrics := range processedMetricsArray {
		if err := metricsRcvr.parseAndProcessMetrics(ctx, processedMetrics, device.SerialNumber, system, device, config); err != nil {
			metricsRcvr.logger.Error("Failed to process metrics",
				zap.String("deviceID", device.ResourceID),
				zap.Error(err),
			)
			return fmt.Errorf("failed to process metrics for %s: %w", device.ResourceID, err)
		}

		// Log successful processing of metrics
		metricsRcvr.logger.Info("Successfully processed metrics for logical unit",
			zap.String("deviceID", device.ResourceID),
		)
	}

	return nil
}

// Fetch and process metrics for a single port.
func (metricsRcvr *metricsReceiver) fetchAndProcessPortMetrics(ctx context.Context, ports []string, system System, device StorageDevice, mu *sync.Mutex, aggregatedErrors *[]error, config ScrapeConfig) error {
	// Log the start time for this batch of ports
	portStartTime := time.Now()
	metricsRcvr.logger.Info("Starting fetch of port metrics", zap.Strings("portIds", ports))

	// Construct the port metrics URL
	portMetricsURL := fmt.Sprintf(endpointFormats["storage:port"].URLFormat, device.GatewayAddress)

	// Create the request body
	requestBody, err := json.Marshal(map[string][]string{"ports": ports})
	if err != nil {
		return fmt.Errorf("failed to create request body for ports %v: %w", ports, err)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", portMetricsURL, bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to create port metrics request for ports %v: %w", ports, err)
	}
	metricsRcvr.addCommonHeaders(req, device)

	// Make the HTTP request
	resp, err := metricsRcvr.getHttpClient().Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch port metrics for ports %v: %w", ports, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code %d for ports %v", resp.StatusCode, ports)
	}

	// Parse the port performance response
	var portMetricsResponse map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&portMetricsResponse); err != nil {
		return fmt.Errorf("failed to decode port metrics response for ports %v: %w", ports, err)
	}

	// Parse the metrics using the existing parsePortMetrics function
	storageMetricsParser := &StorageMetricParser{device: device}
	processedMetricsList, err := storageMetricsParser.parsePortMetrics(portMetricsResponse)
	if err != nil {
		return fmt.Errorf("failed to parse port metrics for ports %v: %w", ports, err)
	}

	// Log the time taken for this batch of ports
	portTimeTaken := time.Since(portStartTime)
	metricsRcvr.logger.Info("Completed fetch of port metrics", zap.Strings("ports", ports), zap.Duration("timeTaken", portTimeTaken))

	// Process each port's metrics
	for _, processedMetrics := range processedMetricsList {
		if err := metricsRcvr.parseAndProcessMetrics(ctx, processedMetrics, device.SerialNumber, system, device, config); err != nil {
			mu.Lock()
			*aggregatedErrors = append(*aggregatedErrors, err)
			mu.Unlock()
		}
	}

	return nil
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
	systems, err := metricsRcvr.getAllSystems(token)
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
			for _, device := range system.ComputeDevices {
				wg.Add(1)
				go func(device ComputeDevice) {
					defer wg.Done()
					deviceStartTime := time.Now()
					metricsRcvr.logger.Debug("Fetching compute metrics", zap.String("instance_id", device.ResourceID))
					err := metricsRcvr.fetchComputeMetrics(ctx, token, device.ResourceID, system, device, config)
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

func (metricsRcvr *metricsReceiver) fetchComputeMetrics(ctx context.Context, token string, resourceID string, system System, device interface{}, config ScrapeConfig) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 3) // Buffer of 3 to handle potential errors

	// Concurrently fetch power metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := metricsRcvr.scrapeAndProcessMetrics(ctx, token, "compute:power", resourceID, system, device, config); err != nil {
			errCh <- err
		}
	}()

	// Concurrently fetch fan metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := metricsRcvr.scrapeAndProcessMetrics(ctx, token, "compute:fan", resourceID, system, device, config); err != nil {
			errCh <- err
		}
	}()

	// Concurrently fetch temperature metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := metricsRcvr.scrapeAndProcessMetrics(ctx, token, "compute:temperature", resourceID, system, device, config); err != nil {
			errCh <- err
		}
	}()

	// Wait for all requests to complete
	wg.Wait()
	close(errCh)

	// Check if any errors occurred during the concurrent requests
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func (metricsRcvr *metricsReceiver) scrapeAndProcessMetrics(ctx context.Context, token string, endpoint string, resourceID string, system System, device interface{}, config ScrapeConfig) error {
	format, exists := endpointFormats[endpoint]
	if !exists {
		metricsRcvr.logger.Error("Unknown endpoint format", zap.String("endpoint", endpoint))
		return fmt.Errorf("unknown endpoint format: %s", endpoint)
	}

	metricsURL := fmt.Sprintf(format.URLFormat, system.GatewayAddress)

	// Parse the timeout duration from ScrapeConfig
	timeout, err := time.ParseDuration(config.Timeout)
	if err != nil {
		metricsRcvr.logger.Error("Invalid timeout value in ScrapeConfig", zap.String("timeout", config.Timeout), zap.Error(err))
		return fmt.Errorf("invalid timeout value in ScrapeConfig: %w", err)
	}

	// Create a new context with a timeout for the HTTP request
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, "GET", metricsURL, nil)
	if err != nil {
		metricsRcvr.logger.Error("Failed to create metrics request", zap.String("url", metricsURL), zap.Error(err))
		return fmt.Errorf("failed to create metrics request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Subsystem-User", "admin")
	req.Header.Set("X-Subsystem-Password", "cmb9.admin")
	req.Header.Set("X-Management-IPs", device.(ComputeDevice).BmcAddress)

	// Log the start of the request
	startTime := time.Now()
	metricsRcvr.logger.Info("Fetching metrics",
		zap.String("url", metricsURL),
		zap.String("resourceID", resourceID),
		zap.String("endpoint", endpoint),
		zap.String("deviceType", fmt.Sprintf("%T", device)),
	)

	// Make the HTTP request
	resp, err := metricsRcvr.getHttpClient().Do(req)
	if err != nil {
		metricsRcvr.logger.Error("Failed to fetch metrics",
			zap.String("url", metricsURL),
			zap.String("resourceID", resourceID),
			zap.Error(err),
		)
		return fmt.Errorf("failed to fetch metrics: %w", err)
	}
	defer resp.Body.Close()

	// Log response time and status code
	duration := time.Since(startTime)
	metricsRcvr.logger.Info("Metrics response received",
		zap.String("url", metricsURL),
		zap.String("resourceID", resourceID),
		zap.Int("status_code", resp.StatusCode),
		zap.Duration("duration", duration),
	)

	// Check if the response is successful
	if resp.StatusCode != http.StatusOK {
		metricsRcvr.logger.Error("Non-OK HTTP status", zap.Int("status_code", resp.StatusCode), zap.String("url", metricsURL))
		return fmt.Errorf("received non-OK HTTP status: %d", resp.StatusCode)
	}

	// Parse the JSON response
	var response map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		metricsRcvr.logger.Error("Failed to decode metrics response",
			zap.String("url", metricsURL),
			zap.String("resourceID", resourceID),
			zap.Error(err),
		)
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Log success in decoding response
	metricsRcvr.logger.Debug("Successfully decoded metrics response", zap.String("resourceID", resourceID), zap.String("endpoint", endpoint))

	// Process the response based on the endpoint
	switch endpoint {
	case "compute:power":
		powerData, ok := response["powerConsumption"].(map[string]interface{})
		if !ok {
			metricsRcvr.logger.Error("Unexpected power consumption structure", zap.String("resourceID", resourceID))
			return fmt.Errorf("unexpected power consumption structure")
		}
		metricsRcvr.logger.Debug("Processing power consumption metrics", zap.String("resourceID", resourceID))
		return metricsRcvr.parseAndProcessMetrics(requestCtx, powerData, resourceID, system, device, config)

	case "compute:temperature":
		sensors, ok := response["sensors"].([]interface{})
		if !ok {
			metricsRcvr.logger.Error("Unexpected temperature sensor data structure", zap.String("resourceID", resourceID))
			return fmt.Errorf("unexpected temperature sensor data structure")
		}
		metricsRcvr.logger.Debug("Processing temperature metrics", zap.String("resourceID", resourceID))
		for _, sensor := range sensors {
			sensorData, ok := sensor.(map[string]interface{})
			if !ok {
				metricsRcvr.logger.Error("Unexpected sensor structure", zap.String("resourceID", resourceID))
				return fmt.Errorf("unexpected sensor structure")
			}

			sensorData["sensor_temperature"] = sensorData["value"]
			delete(sensorData, "value")

			if err := metricsRcvr.parseAndProcessMetrics(requestCtx, sensorData, resourceID, system, device, config); err != nil {
				metricsRcvr.logger.Error("Failed to process temperature metrics", zap.String("resourceID", resourceID), zap.Error(err))
				return err
			}
		}

	case "compute:fan":
		sensors, ok := response["sensors"].([]interface{})
		if !ok {
			metricsRcvr.logger.Error("Unexpected fan sensor data structure", zap.String("resourceID", resourceID))
			return fmt.Errorf("unexpected fan sensor data structure")
		}
		metricsRcvr.logger.Debug("Processing fan metrics", zap.String("resourceID", resourceID))
		for _, sensor := range sensors {
			sensorData, ok := sensor.(map[string]interface{})
			if !ok {
				metricsRcvr.logger.Error("Unexpected sensor structure", zap.String("resourceID", resourceID))
				return fmt.Errorf("unexpected sensor structure")
			}

			sensorData["sensor_fan_value"] = sensorData["value"]
			delete(sensorData, "value")

			if err := metricsRcvr.parseAndProcessMetrics(requestCtx, sensorData, resourceID, system, device, config); err != nil {
				metricsRcvr.logger.Error("Failed to process fan metrics", zap.String("resourceID", resourceID), zap.Error(err))
				return err
			}
		}
	}

	metricsRcvr.logger.Info("Successfully processed metrics", zap.String("resourceID", resourceID), zap.String("endpoint", endpoint))
	return nil
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
