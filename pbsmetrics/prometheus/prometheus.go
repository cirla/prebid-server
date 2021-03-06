package prometheusmetrics

import (
	"time"

	"github.com/prebid/prebid-server/config"
	"github.com/prebid/prebid-server/openrtb_ext"
	"github.com/prebid/prebid-server/pbsmetrics"
	"github.com/prometheus/client_golang/prometheus"
	_ "github.com/prometheus/client_golang/prometheus/promhttp"
)

// Defines the actual Prometheus metrics we will be using. Satisfies interface MetricsEngine
type Metrics struct {
	Registry      *prometheus.Registry
	connCounter   prometheus.Gauge
	connError     *prometheus.CounterVec
	imps          *prometheus.CounterVec
	requests      *prometheus.CounterVec
	reqTimer      *prometheus.HistogramVec
	adaptRequests *prometheus.CounterVec
	adaptTimer    *prometheus.HistogramVec
	adaptBids     *prometheus.CounterVec
	adaptPrices   *prometheus.HistogramVec
	adaptErrors   *prometheus.CounterVec
	cookieSync    prometheus.Counter
	userID        *prometheus.CounterVec
}

// NewMetrics constructs the appropriate options for the Prometheus metrics. Needs to be fed the promethus config
// Its own function to keep the metric creation function cleaner.
func NewMetrics(cfg config.PrometheusMetrics) *Metrics {
	// define the buckets for timers
	timerBuckets := prometheus.LinearBuckets(0.05, 0.05, 20)
	timerBuckets = append(timerBuckets, []float64{1.5, 2.0, 3.0, 5.0, 10.0, 50.0}...)

	standardLabelNames := []string{"demand_source", "request_type", "browser", "cookie", "response_status"}

	adapterLabelNames := []string{"demand_source", "request_type", "browser", "cookie", "adapter_bid", "adapter"}
	bidLabelNames := []string{"demand_source", "request_type", "browser", "cookie", "adapter_bid", "adapter", "bidtype", "markup_type"}
	errorLabelNames := []string{"demand_source", "request_type", "browser", "cookie", "adapter_error", "adapter"}

	metrics := Metrics{}
	metrics.Registry = prometheus.NewRegistry()
	metrics.connCounter = newConnCounter(cfg)
	metrics.Registry.MustRegister(metrics.connCounter)
	metrics.connError = newCounter(cfg, "active_connections_total",
		"Errors reported on the connections coming in.",
		[]string{"ErrorType"},
	)
	metrics.Registry.MustRegister(metrics.connError)
	metrics.imps = newCounter(cfg, "imps_requested_total",
		"Total number of impressions requested through PBS.",
		standardLabelNames,
	)
	metrics.Registry.MustRegister(metrics.imps)
	metrics.requests = newCounter(cfg, "requests_total",
		"Total number of requests made to PBS.",
		standardLabelNames,
	)
	metrics.Registry.MustRegister(metrics.requests)
	metrics.reqTimer = newHistogram(cfg, "request_time_seconds",
		"Seconds to resolve each PBS request.",
		standardLabelNames, timerBuckets,
	)
	metrics.Registry.MustRegister(metrics.reqTimer)
	metrics.adaptRequests = newCounter(cfg, "adapter_requests_total",
		"Number of requests sent out to each bidder.",
		adapterLabelNames,
	)
	metrics.Registry.MustRegister(metrics.adaptRequests)
	metrics.adaptTimer = newHistogram(cfg, "adapter_time_seconds",
		"Seconds to resolve each request to a bidder.",
		adapterLabelNames, timerBuckets,
	)
	metrics.Registry.MustRegister(metrics.adaptTimer)
	metrics.adaptBids = newCounter(cfg, "adapter_bids_recieved_total",
		"Number of bids recieved from each bidder.",
		bidLabelNames,
	)
	metrics.Registry.MustRegister(metrics.adaptBids)
	metrics.adaptPrices = newHistogram(cfg, "adapter_prices",
		"Values of the bids from each bidder.",
		adapterLabelNames, prometheus.LinearBuckets(0.1, 0.1, 200),
	)
	metrics.Registry.MustRegister(metrics.adaptPrices)
	metrics.adaptErrors = newCounter(cfg, "adapter_errors_total",
		"Number of unique error types seen in each request to an adapter.",
		errorLabelNames,
	)
	metrics.Registry.MustRegister(metrics.adaptErrors)
	metrics.cookieSync = newCookieSync(cfg)
	metrics.Registry.MustRegister(metrics.cookieSync)
	metrics.userID = newCounter(cfg, "usersync_total",
		"Number of user ID syncs performed",
		[]string{"action", "bidder"},
	)
	metrics.Registry.MustRegister(metrics.userID)

	initializeTimeSeries(&metrics)

	return &metrics
}

func newConnCounter(cfg config.PrometheusMetrics) prometheus.Gauge {
	opts := prometheus.GaugeOpts{
		Namespace: cfg.Namespace,
		Subsystem: cfg.Subsystem,
		Name:      "active_connections",
		Help:      "Current number of active (open) connections.",
	}
	return prometheus.NewGauge(opts)
}

func newCookieSync(cfg config.PrometheusMetrics) prometheus.Counter {
	opts := prometheus.CounterOpts{
		Namespace: cfg.Namespace,
		Subsystem: cfg.Subsystem,
		Name:      "cookie_sync_requests_total",
		Help:      "Number of cookie sync requests recieved.",
	}
	return prometheus.NewCounter(opts)
}

func newCounter(cfg config.PrometheusMetrics, name string, help string, labels []string) *prometheus.CounterVec {
	opts := prometheus.CounterOpts{
		Namespace: cfg.Namespace,
		Subsystem: cfg.Subsystem,
		Name:      name,
		Help:      help,
	}
	return prometheus.NewCounterVec(opts, labels)
}

func newHistogram(cfg config.PrometheusMetrics, name string, help string, labels []string, buckets []float64) *prometheus.HistogramVec {
	opts := prometheus.HistogramOpts{
		Namespace: cfg.Namespace,
		Subsystem: cfg.Subsystem,
		Name:      name,
		Help:      help,
		Buckets:   buckets,
	}
	return prometheus.NewHistogramVec(opts, labels)
}

func (me *Metrics) RecordConnectionAccept(success bool) {
	if success {
		me.connCounter.Inc()
	} else {
		me.connError.WithLabelValues("accept_error").Inc()
	}

}

func (me *Metrics) RecordConnectionClose(success bool) {
	if success {
		me.connCounter.Dec()
	} else {
		me.connError.WithLabelValues("close_error").Inc()
	}
}

func (me *Metrics) RecordRequest(labels pbsmetrics.Labels) {
	me.requests.With(resolveLabels(labels)).Inc()
}

func (me *Metrics) RecordImps(labels pbsmetrics.Labels, numImps int) {
	me.imps.With(resolveLabels(labels)).Add(float64(numImps))
}

func (me *Metrics) RecordRequestTime(labels pbsmetrics.Labels, length time.Duration) {
	time := float64(length) / float64(time.Second)
	me.reqTimer.With(resolveLabels(labels)).Observe(time)
}

func (me *Metrics) RecordAdapterRequest(labels pbsmetrics.AdapterLabels) {
	me.adaptRequests.With(resolveAdapterLabels(labels)).Inc()
	for k, _ := range labels.AdapterErrors {
		me.adaptErrors.With(resolveAdapterErrorLabels(labels, string(k))).Inc()
	}
}

func (me *Metrics) RecordAdapterBidReceived(labels pbsmetrics.AdapterLabels, bidType openrtb_ext.BidType, hasAdm bool) {
	me.adaptBids.With(resolveBidLabels(labels, bidType, hasAdm)).Inc()
}

func (me *Metrics) RecordAdapterPrice(labels pbsmetrics.AdapterLabels, cpm float64) {
	me.adaptPrices.With(resolveAdapterLabels(labels)).Observe(cpm)
}

func (me *Metrics) RecordAdapterTime(labels pbsmetrics.AdapterLabels, length time.Duration) {
	time := float64(length) / float64(time.Second)
	me.adaptTimer.With(resolveAdapterLabels(labels)).Observe(time)
}

func (me *Metrics) RecordCookieSync(labels pbsmetrics.Labels) {
	me.cookieSync.Inc()
}

func (me *Metrics) RecordUserIDSet(userLabels pbsmetrics.UserLabels) {
	me.userID.With(resolveUserSyncLabels(userLabels)).Inc()
}

func resolveLabels(labels pbsmetrics.Labels) prometheus.Labels {
	return prometheus.Labels{
		"demand_source": string(labels.Source),
		"request_type":  string(labels.RType),
		// "pubid":   labels.PubID,
		"browser":         string(labels.Browser),
		"cookie":          string(labels.CookieFlag),
		"response_status": string(labels.RequestStatus),
	}
}

func resolveAdapterLabels(labels pbsmetrics.AdapterLabels) prometheus.Labels {
	return prometheus.Labels{
		"demand_source": string(labels.Source),
		"request_type":  string(labels.RType),
		// "pubid":   labels.PubID,
		"browser":     string(labels.Browser),
		"cookie":      string(labels.CookieFlag),
		"adapter_bid": string(labels.AdapterBids),
		"adapter":     string(labels.Adapter),
	}
}

func resolveBidLabels(labels pbsmetrics.AdapterLabels, bidType openrtb_ext.BidType, hasAdm bool) prometheus.Labels {
	bidLabels := prometheus.Labels{
		"demand_source": string(labels.Source),
		"request_type":  string(labels.RType),
		// "pubid":   labels.PubID,
		"browser":     string(labels.Browser),
		"cookie":      string(labels.CookieFlag),
		"adapter_bid": string(labels.AdapterBids),
		"adapter":     string(labels.Adapter),
		"bidtype":     string(bidType),
		"markup_type": "unknown",
	}
	if hasAdm {
		bidLabels["markup_type"] = "adm"
	}
	return bidLabels
}

func resolveAdapterErrorLabels(labels pbsmetrics.AdapterLabels, errorType string) prometheus.Labels {
	return prometheus.Labels{
		"demand_source": string(labels.Source),
		"request_type":  string(labels.RType),
		// "pubid":   labels.PubID,
		"browser":       string(labels.Browser),
		"cookie":        string(labels.CookieFlag),
		"adapter_error": errorType,
		"adapter":       string(labels.Adapter),
	}
}

func resolveUserSyncLabels(userLabels pbsmetrics.UserLabels) prometheus.Labels {
	return prometheus.Labels{
		"action": string(userLabels.Action),
		"bidder": string(userLabels.Bidder),
	}
}

// initializeTimeSeries precreates all possible metric label values, so there is no locking needed at run time creating new instances
func initializeTimeSeries(m *Metrics) {
	// Connection errors
	labels := addDimension([]prometheus.Labels{}, "ErrorType", []string{"accept_error", "close_error"})
	for _, l := range labels {
		_ = m.connError.With(l)
	}

	// Standard labels
	labels = addDimension([]prometheus.Labels{}, "demand_source", demandTypesAsString())
	labels = addDimension(labels, "request_type", requestTypesAsString())
	labels = addDimension(labels, "browser", browserTypesAsString())
	labels = addDimension(labels, "cookie", cookieTypesAsString())
	adapterLabels := labels // save regenerating these dimensions for adapter status
	labels = addDimension(labels, "response_status", requestStatusesAsString())
	for _, l := range labels {
		_ = m.imps.With(l)
		_ = m.requests.With(l)
		_ = m.reqTimer.With(l)
	}

	// Adapter labels
	labels = addDimension(adapterLabels, "adapter", adaptersAsString())
	errorLabels := labels // save regenerating these dimensions for adapter errors
	labels = addDimension(labels, "adapter_bid", adapterBidsAsString())
	for _, l := range labels {
		_ = m.adaptRequests.With(l)
		_ = m.adaptTimer.With(l)
		_ = m.adaptPrices.With(l)
	}
	// AdapterBid labels
	labels = addDimension(labels, "bidtype", bidTypesAsString())
	labels = addDimension(labels, "markup_type", []string{"unknown", "adm"})
	for _, l := range labels {
		_ = m.adaptBids.With(l)
	}
	labels = addDimension(errorLabels, "adapter_error", adapterErrorsAsString())
	for _, l := range labels {
		_ = m.adaptErrors.With(l)
	}
}

// addDimesion will expand a slice of labels to add the dimension of a new set of values for a new label name
func addDimension(labels []prometheus.Labels, field string, values []string) []prometheus.Labels {
	if len(labels) == 0 {
		// We are starting a new slice of labels, so we can't loop.
		return addToLabel(make(prometheus.Labels), field, values)
	}
	newLabels := make([]prometheus.Labels, 0, len(labels)*len(values))
	for _, l := range labels {
		newLabels = append(newLabels, addToLabel(l, field, values)...)
	}
	return newLabels
}

// addToLabel will create a slice of labels adding a set of values tied to a label name.
func addToLabel(label prometheus.Labels, field string, values []string) []prometheus.Labels {
	newLabels := make([]prometheus.Labels, len(values))
	for i, v := range values {
		l := copyLabel(label)
		l[field] = v
		newLabels[i] = l
	}
	return newLabels
}

// Need to be able to deep copy prometheus labels.
func copyLabel(label prometheus.Labels) prometheus.Labels {
	newLabel := make(prometheus.Labels)
	for k, v := range label {
		newLabel[k] = v
	}
	return newLabel
}

func demandTypesAsString() []string {
	list := pbsmetrics.DemandTypes()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output
}

func requestTypesAsString() []string {
	list := pbsmetrics.RequestTypes()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output
}

func browserTypesAsString() []string {
	list := pbsmetrics.BrowserTypes()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output
}

func cookieTypesAsString() []string {
	list := pbsmetrics.CookieTypes()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output
}

func requestStatusesAsString() []string {
	list := pbsmetrics.RequestStatuses()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output
}

func adapterBidsAsString() []string {
	list := pbsmetrics.AdapterBids()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output
}

func adapterErrorsAsString() []string {
	list := pbsmetrics.AdapterErrors()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output
}

func adaptersAsString() []string {
	list := openrtb_ext.BidderList()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output

}

func bidTypesAsString() []string {
	list := openrtb_ext.BidTypes()
	output := make([]string, len(list))
	for i, s := range list {
		output[i] = string(s)
	}
	return output

}
