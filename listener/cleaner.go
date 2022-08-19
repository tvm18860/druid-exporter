package listener

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	cron "github.com/robfig/cron/v3"
)

type metricIdentifier struct {
	name   string
	labels prometheus.Labels
}

type cleaner struct {
	m          *sync.Map
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
	minutes    time.Duration
}

func newCleaner(gauges map[string]*prometheus.GaugeVec, histograms map[string]*prometheus.HistogramVec, minutes int) cleaner {
	c := cleaner{
		m:          &sync.Map{},
		gauges:     gauges,
		histograms: histograms,
		minutes:    time.Duration(minutes),
	}

	croner := cron.New()
	_, _ = croner.AddFunc(fmt.Sprintf("@every %dm", minutes), func() {
		c.cleanup()
	})
	croner.Start()
	return c
}

func (c cleaner) add(metric string, labels prometheus.Labels) {
	bytes, err := json.Marshal(metricIdentifier{name: metric, labels: labels})
	if err != nil {
		logrus.Errorf("marshal labels error: %v", err)
		return
	}
	c.m.Store(string(bytes), time.Now())
}

func (c cleaner) cleanup() {
	beforeTime := time.Now().Add(-time.Minute * c.minutes)
	tobeDeletes := make([]interface{}, 0)
	c.m.Range(func(k, v interface{}) bool {
		var metric metricIdentifier
		err := json.Unmarshal([]byte(k.(string)), &metric)
		if err != nil {
			logrus.Errorf("unmarshal labels error: %v", err)
			return true
		}
		updatedAt := v.(time.Time)

		if updatedAt.Before(beforeTime) {
			if _, present := c.gauges[metric.name]; present {
				c.gauges[metric.name].Delete(metric.labels)
			}
			if _, present := c.histograms[metric.name]; present {
				c.histograms[metric.name].Delete(metric.labels)
			}
			tobeDeletes = append(tobeDeletes, metric)
		}
		return true
	})

	for _, tobeDelete := range tobeDeletes {
		c.m.Delete(tobeDelete)
	}
}
