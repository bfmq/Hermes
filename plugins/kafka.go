package plugins

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"strings"
	"time"
	"bfmq/Hermes/conf"
	"github.com/donnie4w/go-logger/logger"
	"github.com/sdbaiguanghe/glog"
	"encoding/json"
)

type FrontData struct {
	LoadTime             int	`json:"loadTime"`
	UnloadEventTime      int	`json:"unloadEventTime"`
	LoadEventTime        int	`json:"loadEventTime"`
	DomReadyTime         int	`json:"domReadyTime"`
	FirstScreen          int	`json:"firstScreen"`
	ParseDomTime         int	`json:"parseDomTime"`
	InitDomTreeTime      int	`json:"initDomTreeTime"`
	ReadyStart           int	`json:"readyStart"`
	RedirectTime         int	`json:"redirectTime"`
	AppcacheTime         int	`json:"appcacheTime"`
	LookupDomainTime     int	`json:"lookupDomainTime"`
	ConnectTime          int	`json:"connectTime"`
	RequestTime          int	`json:"requestTime"`
	RequestDocumentTime  int	`json:"requestDocumentTime"`
	ResponseDocumentTime int	`json:"responseDocumentTime"`
	TTFB                 int	`json:"ttfb"`
	IpAddr               string	`json:"ipAddr"`
	City                 string	`json:"city"`
	Url                  string	`json:"url"`
}

func NewFrontData() (frontData *FrontData) {
	return &FrontData{}
}

func InitKafkaConsumer() (c *cluster.Consumer, err error) {
	groupID := "group-1"
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest //初始从最新的offset开始

	c, err = cluster.NewConsumer(strings.Split(conf.KafkaServer, ","), groupID, strings.Split(conf.Topics, ","), config)
	if err != nil {
		logger.Error("Init Kafka Consumer fail!")
		return
	}

	go func(c *cluster.Consumer) {
		errors := c.Errors()
		noti := c.Notifications()
		for {
			select {
			case err := <-errors:
				logger.Error(err)
			case <-noti:
			}
		}
	}(c)

	return
}

func InitKafkaProducer()(p sarama.AsyncProducer, err error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true //必须有这个选项
	config.Producer.Timeout = 5 * time.Second
	p, err = sarama.NewAsyncProducer(strings.Split(conf.KafkaServer, ","), config)
	if err != nil {
		return
	}

	//必须有这个匿名函数内容
	go func(p sarama.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					glog.Errorln(err)
				}
			case <-success:
			}
		}
	}(p)

	return
}

func SendData2Kafka(frontData *FrontData,p sarama.AsyncProducer)(bool) {
	v,err := json.Marshal(frontData)
	if err!=nil{
		return false
	}
	msg := &sarama.ProducerMessage{
		Topic: conf.Topics,
		Value: sarama.ByteEncoder(v),
	}
	p.Input() <- msg
	return true
}
