package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	JobsEnqueued  *prometheus.CounterVec
	JobsSucceeded *prometheus.CounterVec
	JobsFailed    *prometheus.CounterVec
	JobsDLQ       *prometheus.CounterVec
	JobDuration   *prometheus.HistogramVec
	WorkersActive prometheus.Gauge
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		JobsEnqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_jobs_enqueued_total",
			Help: "Total jobs enqueued.",
		}, []string{"queue", "kind"}),
		JobsSucceeded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_jobs_succeeded_total",
			Help: "Total jobs completed successfully.",
		}, []string{"queue", "kind"}),
		JobsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_jobs_failed_total",
			Help: "Total jobs that failed and were rescheduled for retry.",
		}, []string{"queue", "kind"}),
		JobsDLQ: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "goqueue_jobs_dlq_total",
			Help: "Total jobs moved to the dead-letter queue.",
		}, []string{"queue", "kind"}),
		JobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "goqueue_job_duration_seconds",
			Help:    "Job handler execution time in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"queue", "kind"}),
		WorkersActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "goqueue_workers_active",
			Help: "Number of workers currently executing a job.",
		}),
	}
	reg.MustRegister(
		m.JobsEnqueued,
		m.JobsSucceeded,
		m.JobsFailed,
		m.JobsDLQ,
		m.JobDuration,
		m.WorkersActive,
	)
	return m
}
