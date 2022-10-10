package collector

import (
	"druid-exporter/utils"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	druid = kingpin.Flag(
		"druid.uri",
		"URL of druid router or coordinator, EnvVar - DRUID_URL",
	).Default("http://druid.opstreelabs.in").OverrideDefaultFromEnvar("DRUID_URL").Short('d').String()

	druidTaskStuckThresholdMinutes = kingpin.Flag(
		"druid.stuck.task.threshold.minutes",
		"Threshold in minutes to consider a task stuck, EnvVar - DRUID_STUCK_TASK_THRESHOLD_MINUTES",
	).Default("90").OverrideDefaultFromEnvar("DRUID_STUCK_TASK_THRESHOLD_MINUTES").Float()

	DruidTaskDurationHist = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "druid_task_duration_ms",
			Help:    "Histogram of druid task durations",
			Buckets: []float64{100, 500, 1000, 10000, 60000, 600000, 3600000},
		},
		[]string{"datasource", "status", "task_type"},
	)
)

// GetDruidHealthMetrics returns the set of metrics for druid
func GetDruidHealthMetrics() float64 {
	kingpin.Parse()
	druidHealthURL := *druid + healthURL
	logrus.Debugf("Successfully collected the data for druid healthcheck")
	return utils.GetHealth(druidHealthURL)
}

// GetDruidSegmentData returns the datasources of druid
func GetDruidSegmentData() SegementInterface {
	kingpin.Parse()
	druidSegmentURL := *druid + segmentDataURL
	responseData, err := utils.GetResponse(druidSegmentURL, "Segment")
	if err != nil {
		logrus.Errorf("Cannot collect data for druid segments: %v", err)
		return nil
	}
	logrus.Debugf("Successfully collected the data for druid segment")
	var metric SegementInterface
	err = json.Unmarshal(responseData, &metric)
	if err != nil {
		logrus.Errorf("Cannot parse JSON data: %v", err)
		return nil
	}
	logrus.Debugf("Druid segment's metric data, %v", metric)
	return metric
}

// GetDruidData return all the tasks and its state
func GetDruidData(pathURL string) []map[string]interface{} {
	kingpin.Parse()
	druidURL := *druid + pathURL
	responseData, err := utils.GetResponse(druidURL, pathURL)
	if err != nil {
		logrus.Errorf("Cannot collect data for druid's supervisors: %v", err)
		return nil
	}
	logrus.Debugf("Successfully collected the data for druid's supervisors")
	var metric []map[string]interface{}
	err = json.Unmarshal(responseData, &metric)
	if err != nil {
		logrus.Errorf("Cannot parse JSON data: %v", err)
		return nil
	}
	logrus.Debugf("Druid supervisor's metric data, %v", metric)
	return metric
}

// GetDruidTasksData return all the tasks and its state
func GetDruidTasksData(pathURL string) TasksInterface {
	kingpin.Parse()
	druidURL := *druid + pathURL
	responseData, err := utils.GetResponse(druidURL, pathURL)
	if err != nil {
		logrus.Errorf("Cannot retrieve data for druid's tasks: %v", err)
		return nil
	}
	logrus.Debugf("Successfully retrieved the data for druid's tasks")
	var metric TasksInterface
	err = json.Unmarshal(responseData, &metric)
	if err != nil {
		logrus.Errorf("Cannot parse JSON data: %v", err)
		return nil
	}
	logrus.Debugf("Druid tasks's metric data, %v", metric)
	return metric
}

// GetDruidDataSourcesTotalRows returns the amount of rows in each datasource
func GetDruidDataSourcesTotalRows(pathURL string) DataSourcesTotalRows {
	kingpin.Parse()
	druidURL := *druid + pathURL
	responseData, err := utils.GetSQLResponse(druidURL, totalRowsSQL)
	if err != nil {
		logrus.Errorf("Cannot retrieve data for druid's datasources rows: %v", err)
		return nil
	}
	logrus.Debugf("Successfully retrieved the data for druid's datasources rows")
	var datasources DataSourcesTotalRows
	err = json.Unmarshal(responseData, &datasources)
	if err != nil {
		logrus.Errorf("Cannot parse JSON data: %v", err)
		return nil
	}
	logrus.Debugf("Druid datasources total rows, %v", datasources)
	return datasources
}

// GetDruidTasksStatusCount returns count of different tasks by status
func GetDruidTasksStatusCount(pathURL string) TaskStatusMetric {
	kingpin.Parse()
	druidURL := *druid + pathURL
	responseData, err := utils.GetResponse(druidURL, pathURL)
	if err != nil {
		logrus.Errorf("Cannot retrieve data for druid's workers: %v", err)
		return nil
	}
	logrus.Debugf("Successfully retrieved the data for druid task: %v", pathURL)
	var taskCount TaskStatusMetric
	err = json.Unmarshal(responseData, &taskCount)
	if err != nil {
		logrus.Errorf("Cannot parse JSON data: %v", err)
		return nil
	}
	logrus.Debugf("Successfully collected tasks status count: %v", pathURL)
	return taskCount
}

// getDruidWorkersData return all the workers and its state
func getDruidWorkersData(pathURL string) []worker {
	kingpin.Parse()
	druidURL := *druid + pathURL
	responseData, err := utils.GetResponse(druidURL, pathURL)
	if err != nil {
		logrus.Errorf("Cannot retrieve data for druid's workers: %v", err)
		return nil
	}
	logrus.Debugf("Successfully retrieved the data for druid's workers")
	var workers []worker
	err = json.Unmarshal(responseData, &workers)
	if err != nil {
		logrus.Errorf("Cannot parse JSON data: %v", err)
		return nil
	}
	logrus.Debugf("Druid workers's metric data, %v", workers)

	return workers
}

// Describe will associate the value for druid exporter
func (collector *MetricCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- collector.DruidHealthStatus
	ch <- collector.DataSourceCount
	ch <- collector.DruidSupervisors
	ch <- collector.DruidSegmentCount
	ch <- collector.DruidSegmentSize
	ch <- collector.DruidWorkersCapacityMax
	ch <- collector.DruidWorkersCapacityUsed
	ch <- collector.DruidTaskStuckRuntime
	ch <- collector.DruidSegmentReplicateSize
	ch <- collector.DruidRunningTasks
	ch <- collector.DruidWaitingTasks
	ch <- collector.DruidCompletedTasks
	ch <- collector.DruidPendingTasks
}

// Collector return the defined metrics
func Collector() *MetricCollector {
	return &MetricCollector{
		DruidHealthStatus: prometheus.NewDesc("druid_health_status",
			"Health of Druid, 1 is healthy 0 is not",
			nil, prometheus.Labels{
				"druid": "health",
			},
		),
		DataSourceCount: prometheus.NewDesc("druid_datasource",
			"Datasources present",
			[]string{"datasource"}, nil,
		),
		DruidWorkersCapacityMax: prometheus.NewDesc("druid_workers_capacity_max",
			"Druid workers capacity max",
			[]string{"version", "ip"}, nil,
		),
		DruidWorkersCapacityUsed: prometheus.NewDesc("druid_workers_capacity_used",
			"Druid workers capacity used",
			[]string{"version", "ip"}, nil,
		),
		DruidTaskStuckRuntime: prometheus.NewDesc("druid_task_stuck_runtime_ms",
			"Druid task runtime by task id for long running tasks",
			[]string{"datasource", "task_type", "task_id"}, nil,
		),
		DruidSupervisors: prometheus.NewDesc("druid_supervisors",
			"Druid supervisors status",
			[]string{"supervisor_name", "healthy", "state"}, nil,
		),
		DruidSegmentCount: prometheus.NewDesc("druid_segment_count",
			"Druid segment count",
			[]string{"datasource_name"}, nil,
		),
		DruidSegmentSize: prometheus.NewDesc("druid_segment_size",
			"Druid segment size",
			[]string{"datasource_name"}, nil,
		),
		DruidSegmentReplicateSize: prometheus.NewDesc("druid_segment_replicated_size",
			"Druid segment replicated size",
			[]string{"datasource_name"}, nil,
		),
		DruidDataSourcesTotalRows: prometheus.NewDesc("druid_datasource_total_rows",
			"Number of rows in a datasource",
			[]string{"datasource_name", "source"}, nil),
		DruidRunningTasks: prometheus.NewDesc("druid_running_tasks",
			"Druid running tasks count",
			nil, nil,
		),
		DruidWaitingTasks: prometheus.NewDesc("druid_waiting_tasks",
			"Druid waiting tasks count",
			nil, nil,
		),
		DruidCompletedTasks: prometheus.NewDesc("druid_completed_tasks",
			"Druid completed tasks count",
			nil, nil,
		),
		DruidPendingTasks: prometheus.NewDesc("druid_pending_tasks",
			"Druid pending tasks count",
			nil, nil,
		),
	}
}

// Collect will collect all the metrics
func (collector *MetricCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(collector.DruidHealthStatus,
		prometheus.CounterValue, GetDruidHealthMetrics())
	for _, data := range GetDruidSegmentData() {
		ch <- prometheus.MustNewConstMetric(collector.DataSourceCount,
			prometheus.GaugeValue, float64(1), data.Name)
		if data.Properties.Segments.Count != 0 {
			ch <- prometheus.MustNewConstMetric(collector.DruidSegmentCount,
				prometheus.GaugeValue, float64(data.Properties.Segments.Count), data.Name)
		}
		if data.Properties.Segments.Size != 0 {
			ch <- prometheus.MustNewConstMetric(collector.DruidSegmentSize,
				prometheus.GaugeValue, float64(data.Properties.Segments.Size), data.Name)
		}
		if data.Properties.Segments.ReplicatedSize != 0 {
			ch <- prometheus.MustNewConstMetric(collector.DruidSegmentReplicateSize,
				prometheus.GaugeValue, float64(data.Properties.Segments.ReplicatedSize), data.Name)
		}
	}

	ch <- prometheus.MustNewConstMetric(collector.DruidRunningTasks,
		prometheus.GaugeValue, float64(len(GetDruidTasksStatusCount(runningTask))))
	ch <- prometheus.MustNewConstMetric(collector.DruidWaitingTasks,
		prometheus.GaugeValue, float64(len(GetDruidTasksStatusCount(waitingTask))))
	ch <- prometheus.MustNewConstMetric(collector.DruidCompletedTasks,
		prometheus.GaugeValue, float64(len(GetDruidTasksStatusCount(completedTask))))
	ch <- prometheus.MustNewConstMetric(collector.DruidPendingTasks,
		prometheus.GaugeValue, float64(len(GetDruidTasksStatusCount(pendingTask))))

	workers := getDruidWorkersData(workersURL)

	for _, worker := range workers {
		ch <- prometheus.MustNewConstMetric(collector.DruidWorkersCapacityMax,
			prometheus.GaugeValue, float64(worker.Worker.Capacity), worker.Worker.Version, worker.Worker.IP)
		ch <- prometheus.MustNewConstMetric(collector.DruidWorkersCapacityUsed,
			prometheus.GaugeValue, float64(worker.CurrCapacityUsed), worker.Worker.Version, worker.Worker.IP)
	}

	for _, data := range GetDruidTasksData(tasksURL) {
		hostname := ""
		for _, worker := range workers {
			for _, task := range worker.RunningTasks {
				if task == data.ID {
					hostname = worker.hostname()
					break
				}
			}
			if hostname != "" {
				break
			}
		}
		if hostname == "" {
			if len(workers) != 0 {
				hostname = workers[rand.Intn(len(workers))].hostname()
			}
		}

		DruidTaskDurationHist.WithLabelValues(data.DataSource, data.Status, data.Type).Observe(data.Duration)

		// Check for long-running, potentially stuck tasks
		if data.Status == "RUNNING" {
			taskStartTime, err := time.Parse("2006-01-02T15:04:05.000Z", data.CreatedTime)

			if err != nil {
				logrus.Warnf("Unable to parse task start time of " + data.CreatedTime)
			} else {
				runtimeMinutes := time.Now().UTC().Sub(taskStartTime).Minutes()
				if runtimeMinutes >= *druidTaskStuckThresholdMinutes {
					ch <- prometheus.MustNewConstMetric(collector.DruidTaskStuckRuntime,
						prometheus.GaugeValue, runtimeMinutes, data.DataSource, data.Type, data.ID)
				}
			}
		}
	}

	for _, data := range GetDruidData(supervisorURL) {
		ch <- prometheus.MustNewConstMetric(collector.DruidSupervisors,
			prometheus.GaugeValue, float64(1), fmt.Sprintf("%v", data["id"]),
			fmt.Sprintf("%v", data["healthy"]), fmt.Sprintf("%v", data["detailedState"]))
	}

	for _, data := range GetDruidDataSourcesTotalRows(sqlURL) {
		ch <- prometheus.MustNewConstMetric(collector.DruidDataSourcesTotalRows, prometheus.GaugeValue, float64(data.TotalRows), data.Datasource, data.Source)
	}
}
