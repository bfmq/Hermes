package plugins

import (
	"bfmq/Hermes/conf"
	"github.com/donnie4w/go-logger/logger"
	"github.com/influxdata/influxdb/client/v2"
)

var (
	cc  client.Client
	cbp client.BatchPoints
)

func init() {
	coon, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     conf.InfluxDBServer,
		Username: conf.InfluxDBUsername,
		Password: conf.InfluxDBPassword,
	})
	if err != nil {
		logger.Error("Init InfluxDB fail!")
		return
	}
	cc = coon

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  conf.InfluxDBName,
		Precision: "s",
	})
	if err != nil {
		logger.Error("NewBatchPoints fail!")
	}
	cbp = bp
}

func InsertData2InfluxDB(frontData *FrontData) {
	tags := map[string]string{"url": frontData.Url}
	fields := map[string]interface{}{
		"appcacheTime":         frontData.AppcacheTime,
		"loadEventTime":        frontData.LoadEventTime,
		"lookupDomainTime":     frontData.LookupDomainTime,
		"initDomTreeTime":      frontData.InitDomTreeTime,
		"connectTime":          frontData.ConnectTime,
		"parseDomTime":         frontData.ParseDomTime,
		"responseDocumentTime": frontData.ResponseDocumentTime,
		"requestDocumentTime":  frontData.RequestDocumentTime,
		"domReadyTime":         frontData.DomReadyTime,
		"TTFB":                 frontData.TTFB,
		"unloadEventTime":      frontData.UnloadEventTime,
		"firstScreen":          frontData.FirstScreen,
		"loadTime":             frontData.LoadTime,
		"requestTime":          frontData.RequestTime,
		"readyStart":           frontData.ReadyStart,
		"redirectTime":         frontData.RedirectTime,
	}

	pt, err := client.NewPoint(frontData.City, tags, fields)
	if err != nil {
		logger.Error("NewPoint fail!")
	}

	cbp.AddPoint(pt)
	if err := cc.Write(cbp); err != nil {
		logger.Error("Write fail!")
	}
}
