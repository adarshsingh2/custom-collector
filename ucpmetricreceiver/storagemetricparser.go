package metricsreceiver

import (
	"fmt"
	"strings"
	"time"
)

type StorageMetricParser struct {
	device StorageDevice
}

type PortResponse struct {
	Data []struct {
		PortId string `json:"portId"`
	} `json:"data"`
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
