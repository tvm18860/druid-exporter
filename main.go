package main

import (
	"druid-exporter/collector"
	"druid-exporter/listener"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

var (
	port = kingpin.Flag(
		"port",
		"Port to listen druid exporter, EnvVar - PORT. (Default - 8080)",
	).Default("8080").OverrideDefaultFromEnvar("PORT").Short('p').String()
	logLevel = kingpin.Flag(
		"log.level",
		"Log level for druid exporter, EnvVar - LOG_LEVEL. (Default: info)",
	).Default("info").OverrideDefaultFromEnvar("LOG_LEVEL").Short('l').String()
	logFormat = kingpin.Flag(
		"log.format",
		"Log format for druid exporter, text or json, EnvVar - LOG_FORMAT. (Default: text)",
	).Default("text").OverrideDefaultFromEnvar("LOG_FORMAT").Short('f').String()
	metricsCleanupTTL = kingpin.Flag(
		"metrics-cleanup-ttl",
		"Flag to provide time in minutes for metrics cleanup.",
	).Default("5").OverrideDefaultFromEnvar("METRICS_CLEANUP_TTL").Int()
	dimensionFilePath = kingpin.Flag(
		"dimension-file-path",
		"Path to file containing desired dimensions for emitted metrics",
	).Default("dimensionMap.json").OverrideDefaultFromEnvar("DIMENSION_FILE_PATH").String()
)

func main() {
	kingpin.Version("0.12")
	kingpin.Parse()
	parsedLevel, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.Errorf("log-level flag has invalid value %s", *logLevel)
	} else {
		logrus.SetLevel(parsedLevel)
	}
	if *logFormat == "json" {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{
			DisableColors: true,
			FullTimestamp: true,
		})
	}

	druidDimensionMap := make(map[string]listener.DimensionMap)
	file, _ := ioutil.ReadFile(*dimensionFilePath)
	err = json.Unmarshal([]byte(file), &druidDimensionMap)
	if err != nil {
		logrus.Errorln(err)
		return
	}

	druidEmittedGauges := make(map[string]*prometheus.GaugeVec)
	druidEmittedHistograms := make(map[string]*prometheus.HistogramVec)
	for metric, opts := range druidDimensionMap {
		promMetric := strings.ReplaceAll(strings.ReplaceAll(metric, "/", "_"), "-", "_")
		labels := append(opts.Dimensions, "host", "service")
		if opts.IncludeAsHistogram {
			vec := promauto.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "druid_emitted_" + promMetric,
					Buckets: opts.Buckets,
				}, labels,
			)
			druidEmittedHistograms[metric] = vec
		} else {
			vec := promauto.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "druid_emitted_" + promMetric,
				}, labels,
			)
			druidEmittedGauges[metric] = vec
		}
	}

	dnsCache := cache.New(5*time.Minute, 10*time.Minute)
	router := mux.NewRouter()
	getDruidAPIdata := collector.Collector()
	prometheus.MustRegister(getDruidAPIdata)

	router.Handle("/druid", listener.DruidHTTPEndpoint(druidDimensionMap, druidEmittedHistograms, druidEmittedGauges, *metricsCleanupTTL, dnsCache))
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>Druid Exporter</title></head>
			<body>
			<h1>Druid Exporter</h1>
			<p><a href="/metrics">Metrics</a></p>
			</body>
			</html>`))
	})
	logrus.Infof("Druid exporter started listening on: %v", *port)
	logrus.Infof("Metrics endpoint - http://0.0.0.0:%v/metrics", *port)
	logrus.Infof("Druid emitter endpoint - http://0.0.0.0:%v/druid", *port)
	http.ListenAndServe("0.0.0.0:"+*port, router)
}
