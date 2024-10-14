package metricsreceiver

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