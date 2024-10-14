package metricsreceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// ComputeMetricParser handles fetching and processing compute metrics
type ComputeMetricParser struct {
	metricsRcvr *metricsReceiver
}

// fetchComputeMetrics fetches the metrics for a compute device.
func (parser *ComputeMetricParser) fetchComputeMetrics(ctx context.Context, token string, resourceID string, system System, device ComputeDevice, config ScrapeConfig) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 3) // Buffer of 3 to handle potential errors

	// Concurrently fetch power metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := parser.parseComputeMetrics(ctx, token, "compute:power", resourceID, system, device, config); err != nil {
			errCh <- err
		}
	}()

	// Concurrently fetch fan metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := parser.parseComputeMetrics(ctx, token, "compute:fan", resourceID, system, device, config); err != nil {
			errCh <- err
		}
	}()

	// Concurrently fetch temperature metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := parser.parseComputeMetrics(ctx, token, "compute:temperature", resourceID, system, device, config); err != nil {
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

// scrapeAndProcessMetrics fetches and processes metrics for a given endpoint.
func (parser *ComputeMetricParser) parseComputeMetrics(ctx context.Context, token string, endpoint string, resourceID string, system System, device ComputeDevice, config ScrapeConfig) error {
	format, exists := endpointFormats[endpoint]
	if !exists {
		parser.metricsRcvr.logger.Error("Unknown endpoint format", zap.String("endpoint", endpoint))
		return fmt.Errorf("unknown endpoint format: %s", endpoint)
	}

	metricsURL := fmt.Sprintf(format.URLFormat, system.GatewayAddress)

	// Parse the timeout duration from ScrapeConfig
	timeout, err := time.ParseDuration(config.Timeout)
	if err != nil {
		parser.metricsRcvr.logger.Error("Invalid timeout value in ScrapeConfig", zap.String("timeout", config.Timeout), zap.Error(err))
		return fmt.Errorf("invalid timeout value in ScrapeConfig: %w", err)
	}

	// Create a new context with a timeout for the HTTP request
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(requestCtx, "GET", metricsURL, nil)
	if err != nil {
		parser.metricsRcvr.logger.Error("Failed to create metrics request", zap.String("url", metricsURL), zap.Error(err))
		return fmt.Errorf("failed to create metrics request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Subsystem-User", "admin")
	req.Header.Set("X-Subsystem-Password", "cmb9.admin")
	req.Header.Set("X-Management-IPs", device.BmcAddress)

	// Log the start of the request
	startTime := time.Now()
	parser.metricsRcvr.logger.Info("Fetching metrics", zap.String("url", metricsURL), zap.String("resourceID", resourceID), zap.String("endpoint", endpoint))

	// Make the HTTP request
	resp, err := parser.metricsRcvr.getHttpClient().Do(req)
	if err != nil {
		parser.metricsRcvr.logger.Error("Failed to fetch metrics", zap.String("url", metricsURL), zap.String("resourceID", resourceID), zap.Error(err))
		return fmt.Errorf("failed to fetch metrics: %w", err)
	}
	defer resp.Body.Close()

	// Log response time and status code
	duration := time.Since(startTime)
	parser.metricsRcvr.logger.Info("Metrics response received", zap.String("url", metricsURL), zap.String("resourceID", resourceID), zap.Int("status_code", resp.StatusCode), zap.Duration("duration", duration))

	if resp.StatusCode != http.StatusOK {
		parser.metricsRcvr.logger.Error("Non-OK HTTP status", zap.Int("status_code", resp.StatusCode), zap.String("url", metricsURL))
		return fmt.Errorf("received non-OK HTTP status: %d", resp.StatusCode)
	}

	// Decode the response
	var response map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		parser.metricsRcvr.logger.Error("Failed to decode metrics response", zap.String("url", metricsURL), zap.String("resourceID", resourceID), zap.Error(err))
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Log success in decoding response
	parser.metricsRcvr.logger.Debug("Successfully decoded metrics response", zap.String("resourceID", resourceID), zap.String("endpoint", endpoint))

	// Process the response based on the endpoint
	switch endpoint {
	case "compute:power":
		powerData, ok := response["powerConsumption"].(map[string]interface{})
		if !ok {
			parser.metricsRcvr.logger.Error("Unexpected power consumption structure", zap.String("resourceID", resourceID))
			return fmt.Errorf("unexpected power consumption structure")
		}
		parser.metricsRcvr.logger.Debug("Processing power consumption metrics", zap.String("resourceID", resourceID))
		return parser.processParsedMetrics(ctx, powerData, resourceID, system, device, config)

	case "compute:temperature":
		sensors, ok := response["sensors"].([]interface{})
		if !ok {
			parser.metricsRcvr.logger.Error("Unexpected temperature sensor data structure", zap.String("resourceID", resourceID))
			return fmt.Errorf("unexpected temperature sensor data structure")
		}
		parser.metricsRcvr.logger.Debug("Processing temperature metrics", zap.String("resourceID", resourceID))
		for _, sensor := range sensors {
			sensorData, ok := sensor.(map[string]interface{})
			if !ok {
				parser.metricsRcvr.logger.Error("Unexpected sensor structure", zap.String("resourceID", resourceID))
				return fmt.Errorf("unexpected sensor structure")
			}

			sensorData["sensor_temperature"] = sensorData["value"]
			delete(sensorData, "value")

			if err := parser.processParsedMetrics(ctx, sensorData, resourceID, system, device, config); err != nil {
				parser.metricsRcvr.logger.Error("Failed to process temperature metrics", zap.String("resourceID", resourceID), zap.Error(err))
				return err
			}
		}

	case "compute:fan":
		sensors, ok := response["sensors"].([]interface{})
		if !ok {
			parser.metricsRcvr.logger.Error("Unexpected fan sensor data structure", zap.String("resourceID", resourceID))
			return fmt.Errorf("unexpected fan sensor data structure")
		}
		parser.metricsRcvr.logger.Debug("Processing fan metrics", zap.String("resourceID", resourceID))
		for _, sensor := range sensors {
			sensorData, ok := sensor.(map[string]interface{})
			if !ok {
				parser.metricsRcvr.logger.Error("Unexpected sensor structure", zap.String("resourceID", resourceID))
				return fmt.Errorf("unexpected sensor structure")
			}

			sensorData["sensor_fan_value"] = sensorData["value"]
			delete(sensorData, "value")

			if err := parser.processParsedMetrics(ctx, sensorData, resourceID, system, device, config); err != nil {
				parser.metricsRcvr.logger.Error("Failed to process fan metrics", zap.String("resourceID", resourceID), zap.Error(err))
				return err
			}
		}
	}

	parser.metricsRcvr.logger.Info("Successfully processed metrics", zap.String("resourceID", resourceID), zap.String("endpoint", endpoint))
	return nil
}

// processParsedMetrics parses the response data and processes the metrics.
func (parser *ComputeMetricParser) processParsedMetrics(ctx context.Context, data map[string]interface{}, instanceID string, system System, device ComputeDevice, config ScrapeConfig) error {
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

	// Add specific labels based on the type of device (ComputeDevice)
	labels.PutStr("device_resource_id", device.ResourceID)
	labels.PutStr("device_bios_version", device.BiosVersion)
	labels.PutStr("device_model", device.Model)
	labels.PutStr("device_serial", device.Serial)

	// First pass: Collect all labels and timestamps
	for key, value := range data {
		if contains(config.TimestampKeys, key) {
			// If the key is defined as a timestamp in the config
			if timestamp, err := time.Parse(time.RFC3339, fmt.Sprintf("%v", value)); err == nil {
				metricTimestamp = pcommon.NewTimestampFromTime(timestamp)
				hasTimestamp = true
			} else {
				parser.metricsRcvr.logger.Warn("Invalid timestamp format", zap.String("key", key), zap.String("value", fmt.Sprintf("%v", value)))
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
				parser.metricsRcvr.logger.Warn("Skipping non-numeric metric value", zap.String("key", key), zap.String("value", fmt.Sprintf("%v", value)))
			}
		}
	}

	// Send the converted metrics to the next consumer
	return parser.metricsRcvr.nextConsumer.ConsumeMetrics(ctx, metrics)
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