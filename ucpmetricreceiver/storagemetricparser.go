package metricsreceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type StorageMetricParser struct {
	device StorageDevice
}

type PortResponse struct {
	Data []struct {
		PortId string `json:"portId"`
	} `json:"data"`
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
		if err := metricsRcvr.fetchLogicalUnitMetrics(ctx, system, device, config); err != nil {
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
func (metricsRcvr *metricsReceiver) fetchLogicalUnitMetrics(ctx context.Context, system System, device StorageDevice, config ScrapeConfig) error {
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

	// Loop through the array of processed metrics and call processParsedMetrics for each
	for _, processedMetrics := range processedMetricsArray {
		if err := metricsRcvr.processParsedMetrics(ctx, processedMetrics, device.SerialNumber, system, device, config); err != nil {
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
		if err := metricsRcvr.processParsedMetrics(ctx, processedMetrics, device.SerialNumber, system, device, config); err != nil {
			mu.Lock()
			*aggregatedErrors = append(*aggregatedErrors, err)
			mu.Unlock()
		}
	}

	return nil
}

// New function to parse port metrics
func (storageMetricsParser *StorageMetricParser) parsePortMetrics(portMetricsResponse map[string]interface{}) ([]map[string]interface{}, error) {
	// Ensure the necessary fields are present
	portPerformanceStats, ok := portMetricsResponse["portsPerformanceStatistics"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("missing or invalid portsPerformanceStatistics in the response")
	}

	// Prepare a slice to hold the processed metrics for each port
	var processedMetricsList []map[string]interface{}

	// Iterate over each port's performance statistics
	for _, portStatsRaw := range portPerformanceStats {
		portStats, ok := portStatsRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid port performance statistics format")
		}

		// Extract necessary data
		portId, ok := portStats["portId"].(string)
		if !ok {
			return nil, fmt.Errorf("missing portId in portPerformanceStatistics")
		}

		portIOPS, ok := portStats["portIOPS"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("missing portIOPS in portPerformanceStatistics")
		}

		portTransferKBPS, ok := portStats["portTransferKBPS"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("missing portTransferKBPS in portPerformanceStatistics")
		}

		// Convert metrics into the required format
		processedMetrics := map[string]interface{}{
			"storageSerial": storageMetricsParser.device.SerialNumber,
			"port":          portId,
			"portIopsAvg":   int(portIOPS["average"].(float64)), // type assertions with proper conversion
			"portIopsMax":   int(portIOPS["maximum"].(float64)),
			"portIopsMin":   int(portIOPS["minimum"].(float64)),
			"portKbpsAvg":   int(portTransferKBPS["average"].(float64)),
			"portKbpsMax":   int(portTransferKBPS["maximum"].(float64)),
			"portKbpsMin":   int(portTransferKBPS["minimum"].(float64)),
			"timeStamp":     time.Now().Format(time.RFC3339),
		}

		// Add processed metrics to the list
		processedMetricsList = append(processedMetricsList, processedMetrics)
	}

	return processedMetricsList, nil
}

// ParseMetrics parses the logical unit performance metrics dynamically from a JSON string
func (storageMetricsParser *StorageMetricParser) parseLogicalUnitMetrics(jsonData map[string]interface{}) ([]map[string]interface{}, error) {
	// Array to store metrics for each logical unit
	var allMetrics []map[string]interface{}

	// Ensure we have a valid array to traverse
	stats, ok := jsonData["logicalUnitsPerformanceStatistics"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("logicalUnitsPerformanceStatistics not found or not an array")
	}

	for _, statItem := range stats {
		stat, ok := statItem.(map[string]interface{})
		if !ok {
			continue // Skip invalid items
		}

		// Create a map for each logical unit's metrics
		metrics := make(map[string]interface{})
		baseKey := "logical_unit"

		// Extract the logicalUnitId
		logicalUnitId, ok := stat["logicalUnitId"].(float64)
		if !ok {
			continue // Skip if logicalUnitId is not present or invalid
		}

		// Add logicalUnitId to the metrics map
		metrics["logicalUnitId"] = int(logicalUnitId)

		// Extract metrics directly and safely
		extractBlockMetrics(stat, baseKey, metrics, "ldevBlockIO")
		extractBlockMetrics(stat, baseKey, metrics, "logicalUnitBlockIO")
		extractIOAndLatencyMetrics(stat, baseKey, metrics, "ldevIO", "ldevLatency")
		extractIOAndLatencyMetrics(stat, baseKey, metrics, "logicalUnitIO", "")

		// Compute additional metrics
		computeAdditionalMetrics(baseKey, metrics, stat)

		// Append the metrics map for this logical unit to the array
		allMetrics = append(allMetrics, metrics)
	}

	// Optionally, print all metrics (this will print each logical unit's metrics separately)
	return allMetrics, nil
}

// extractBlockMetrics extracts block IO metrics into the metrics map
func extractBlockMetrics(stat map[string]interface{}, baseKey string, metrics map[string]interface{}, blockKey string) {
	if blockIO, found := stat[blockKey].(map[string]interface{}); found {
		extractIOMetrics(blockIO, baseKey+"_"+blockKey, metrics)
	}
}

// extractIOAndLatencyMetrics extracts IO and latency metrics into the metrics map
func extractIOAndLatencyMetrics(stat map[string]interface{}, baseKey string, metrics map[string]interface{}, ioKey string, latencyKey string) {
	if ioMetrics, found := stat[ioKey].(map[string]interface{}); found {
		extractIOMetrics(ioMetrics, baseKey+"_"+ioKey, metrics)
	}
	if latencyKey != "" {
		if latencyMetrics, found := stat[latencyKey].(map[string]interface{}); found {
			for key, value := range latencyMetrics {
				metrics[fmt.Sprintf("%s_%s_%s", baseKey, latencyKey, key)] = value
			}
		}
	}
}

// extractIOMetrics is a helper function to extract IO metrics (randomIO, sequentialIO, totalIO)
func extractIOMetrics(ioMap map[string]interface{}, baseKey string, metrics map[string]interface{}) {
	for ioType, ioData := range ioMap {
		if ioStats, ok := ioData.(map[string]interface{}); ok {
			for key, value := range ioStats {
				metrics[fmt.Sprintf("%s_%s_%s", baseKey, ioType, key)] = value
			}
		}
	}
}

// computeAdditionalMetrics computes additional metrics based on the extracted IO metrics
func computeAdditionalMetrics(baseKey string, metrics map[string]interface{}, stat map[string]interface{}) {
	ldevIO := getNestedMap(stat, "ldevIO")
	logicalUnitIO := getNestedMap(stat, "logicalUnitIO")

	// Calculate total read/write I/O
	totalReadIO := getIOValue(ldevIO, "totalIO.read") + getIOValue(logicalUnitIO, "totalIO.read")
	totalWriteIO := getIOValue(ldevIO, "totalIO.write") + getIOValue(logicalUnitIO, "totalIO.write")
	metrics[baseKey+"_total_read_io"] = totalReadIO
	metrics[baseKey+"_total_write_io"] = totalWriteIO

	// Calculate read/write cache hit ratios
	readHits := getIOValue(ldevIO, "totalIO.readHit") + getIOValue(logicalUnitIO, "totalIO.readHit")
	writeHits := getIOValue(ldevIO, "totalIO.writeHit") + getIOValue(logicalUnitIO, "totalIO.writeHit")
	cacheReadRatio := calculateRatio(readHits, totalReadIO)
	cacheWriteRatio := calculateRatio(writeHits, totalWriteIO)
	metrics[baseKey+"_read_cache_hit_ratio"] = cacheReadRatio
	metrics[baseKey+"_write_cache_hit_ratio"] = cacheWriteRatio

	// Calculate IOPS (assumed a time frame for calculation)
	timeFrame := 1.0 // seconds for simplicity
	metrics[baseKey+"_iops"] = (totalReadIO + totalWriteIO) / timeFrame
	metrics[baseKey+"_random_iops"] = getIOValue(ldevIO, "randomIO.read") / timeFrame
	metrics[baseKey+"_sequential_iops"] = getIOValue(ldevIO, "sequentialIO.read") / timeFrame

	// Latency metrics
	metrics[baseKey+"_read_latency"] = getIOValue(ldevIO, "ldevLatency.read")
	metrics[baseKey+"_write_latency"] = getIOValue(ldevIO, "ldevLatency.write")
	metrics[baseKey+"_max_read_latency"] = getIOValue(ldevIO, "ldevLatency.readMax")
	metrics[baseKey+"_max_write_latency"] = getIOValue(ldevIO, "ldevLatency.writeMax")

	// Total Read Hits and Cache Hits
	totalReadHit := readHits
	totalWriteHit := writeHits
	totalCacheHits := totalReadHit + totalWriteHit
	metrics[baseKey+"_total_read_hit"] = totalReadHit
	metrics[baseKey+"_total_write_hit"] = totalWriteHit
	metrics[baseKey+"_total_cache_hits"] = totalCacheHits
}

// Helper function to safely retrieve nested maps
func getNestedMap(data map[string]interface{}, key string) map[string]interface{} {
	if nestedMap, found := data[key].(map[string]interface{}); found {
		return nestedMap
	}
	return nil
}

// Helper function to retrieve IO values safely
func getIOValue(ioMap map[string]interface{}, path string) float64 {
	segments := strings.Split(path, "_")
	var value interface{} = ioMap
	for _, segment := range segments {
		if m, ok := value.(map[string]interface{}); ok {
			value = m[segment]
		} else {
			return 0
		}
	}
	if v, ok := value.(float64); ok {
		return v
	}
	return 0
}

// Helper function to calculate the cache hit ratio
func calculateRatio(hit, total float64) float64 {
	if total == 0 {
		return 0
	}
	return hit / total
}
