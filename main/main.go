package main

import (
	"bfmq/Hermes/conf"
	"bfmq/Hermes/plugins"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/donnie4w/go-logger/logger"
	"net/http"
	"strconv"
	"strings"
)

var frontData *plugins.FrontData
var cc *cluster.Consumer
var sap sarama.AsyncProducer

func init() {
	frontData = plugins.NewFrontData()
	ikc, err := plugins.InitKafkaConsumer()
	if err != nil {
		logger.Error("Init Kafka Consumer fail!")
		return
	}
	cc = ikc

	ikp, err := plugins.InitKafkaProducer()
	if err != nil {
		logger.Error("Init Kafka Consumer fail!")
		return
	}
	sap = ikp

	go func() {
		for msg := range cc.Messages() {
			err := json.Unmarshal(msg.Value, frontData)
			if err != nil {
				continue
			}
			go plugins.InsertData2InfluxDB(frontData)
		}
	}()
}

func insert2Kafka(w http.ResponseWriter, r *http.Request) {
	fd := plugins.NewFrontData()
	r.ParseForm()

	fd.IpAddr = strings.Split(r.RemoteAddr, ":")[0]
	fd.Url = strings.Split(r.Referer(), "/")[2]
	fd.City = ip2City(fd.IpAddr)
	fd.LoadTime, _ = strconv.Atoi(r.Form.Get("loadTime"))
	fd.UnloadEventTime, _ = strconv.Atoi(r.Form.Get("unloadEventTime"))
	fd.LoadEventTime, _ = strconv.Atoi(r.Form.Get("loadEventTime"))
	fd.DomReadyTime, _ = strconv.Atoi(r.Form.Get("domReadyTime"))
	fd.FirstScreen, _ = strconv.Atoi(r.Form.Get("firstScreen"))
	fd.ParseDomTime, _ = strconv.Atoi(r.Form.Get("parseDomTime"))
	fd.InitDomTreeTime, _ = strconv.Atoi(r.Form.Get("initDomTreeTime"))
	fd.ReadyStart, _ = strconv.Atoi(r.Form.Get("readyStart"))
	fd.RedirectTime, _ = strconv.Atoi(r.Form.Get("redirectTime"))
	fd.AppcacheTime, _ = strconv.Atoi(r.Form.Get("appcacheTime"))
	fd.LookupDomainTime, _ = strconv.Atoi(r.Form.Get("lookupDomainTime"))
	fd.ConnectTime, _ = strconv.Atoi(r.Form.Get("connectTime"))
	fd.RequestTime, _ = strconv.Atoi(r.Form.Get("requestTime"))
	fd.RequestDocumentTime, _ = strconv.Atoi(r.Form.Get("requestDocumentTime"))
	fd.ResponseDocumentTime, _ = strconv.Atoi(r.Form.Get("responseDocumentTime"))
	fd.TTFB, _ = strconv.Atoi(r.Form.Get("TTFB"))

	_ = plugins.SendData2Kafka(fd, sap)
}

func main() {
	http.HandleFunc("/", insert2Kafka)
	err := http.ListenAndServe(conf.ServerPort, nil)
	if err != nil {
		logger.Error("start http fail: ", err)
		return
	}
}
