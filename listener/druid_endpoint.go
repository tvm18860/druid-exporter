package listener

import (
	"druid-exporter/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang/gddo/httputil/header"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

type DimensionMap struct {
	Dimensions         []string  `json:"dimensions"`
	IncludeAsHistogram bool      `json:"includeAsHistogram,omitempty"`
	Buckets            []float64 `json:"buckets,omitempty"`
}

// DruidHTTPEndpoint is the endpoint to listen all druid metrics
func DruidHTTPEndpoint(dimensionMap map[string]DimensionMap, histograms map[string]*prometheus.HistogramVec, gauges map[string]*prometheus.GaugeVec, metricsCleanupTTL int, dnsCache *cache.Cache) http.HandlerFunc {
	metricCleaner := newCleaner(gauges, histograms, metricsCleanupTTL)
	return func(w http.ResponseWriter, req *http.Request) {
		var druidData []map[string]interface{}
		reqHeader, _ := header.ParseValueAndParams(req.Header, "Content-Type")
		if req.Method == "POST" && reqHeader == "application/json" {
			output, err := ioutil.ReadAll(req.Body)
			defer req.Body.Close()
			if err != nil {
				logrus.Debugf("Unable to read JSON response: %v", err)
				return
			}
			err = json.Unmarshal(output, &druidData)
			if err != nil {
				logrus.Errorf("Error decoding JSON sent by druid: %v", err)
				if druidData != nil {
					logrus.Debugf("%v", druidData)
				}
				return
			}
			if druidData == nil {
				logrus.Debugf("The dataset for druid is empty, can be ignored: %v", druidData)
				return
			}
			for i, data := range druidData {
				metric := fmt.Sprintf("%v", data["metric"])
				service := fmt.Sprintf("%v", data["service"])
				hostname := fmt.Sprintf("%v", data["host"])
				value, _ := strconv.ParseFloat(fmt.Sprintf("%v", data["value"]), 64)

				// Reverse DNS Lookup
				// Mutates dnsCache
				hostValue := strings.Split(hostname, ":")[0]
				dnsLookupValue := utils.ReverseDNSLookup(hostValue, dnsCache)
				host := strings.Replace(hostname, hostValue, dnsLookupValue, 1) // Adding back port

				if i == 0 { // Comment out this line if you want the whole metrics received
					logrus.Tracef("parameters received: %v", data)
				}

				if opts, ok := dimensionMap[metric]; ok {
					labelMap := map[string]string{"host": host, "service": service}
					for _, l := range opts.Dimensions {
						if data[l] != nil {
							labelMap[l] = fmt.Sprintf("%v", data[l])
						} else {
							labelMap[l] = ""
						}
					}

					if opts.IncludeAsHistogram {
						histograms[metric].With(labelMap).Observe(value)
						metricCleaner.add(metric, labelMap)
					} else {
						gauges[metric].With(labelMap).Set(value)
						metricCleaner.add(metric, labelMap)
					}
				}
			}

			logrus.Infof("Successfully collected data from druid emitter, %s", druidData[0]["service"].(string))
		}
	}
}
