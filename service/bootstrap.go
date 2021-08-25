package service

import (
	"net/http"

	"github.com/HPISTechnologies/component-lib/actor"
	"github.com/HPISTechnologies/component-lib/storage"
	"github.com/HPISTechnologies/component-lib/streamer"
	"github.com/HPISTechnologies/generic-hashing-svc/service/workers"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"

	"github.com/HPISTechnologies/component-lib/kafka"
)

type Config struct {
	concurrency int
	groupid     string
}

//return a Subscriber struct
func NewConfig() *Config {
	return &Config{
		concurrency: viper.GetInt("concurrency"),
		groupid:     "generic-hashing",
	}
}

func (cfg *Config) Start() {
	// logrus.SetLevel(logrus.DebugLevel)
	// logrus.SetFormatter(&logrus.JSONFormatter{})

	http.Handle("/streamer", promhttp.Handler())
	go http.ListenAndServe(":19006", nil)

	broker := streamer.NewStatefulStreamer()
	//00 initializer
	initializer := actor.NewActor(
		"initializer",
		broker,
		[]string{actor.MsgStarting},
		[]string{
			actor.MsgStartSub,
		},
		[]int{1},
		workers.NewInitializer(cfg.concurrency, cfg.groupid),
	)
	initializer.Connect(streamer.NewDisjunctions(initializer, 1))

	receiveMseeages := []string{
		actor.MsgInclusive,
		actor.MsgReceiptHashList,
		actor.MsgAppHash,
	}

	receiveTopics := []string{
		viper.GetString("msgexch"),
		viper.GetString("inclusive-txs"),
		viper.GetString("exec-rcpt-hash"),
	}

	//01 kafkaDownloader
	kafkaDownloader := actor.NewActor(
		"kafkaDownloader",
		broker,
		[]string{actor.MsgStartSub},
		receiveMseeages,
		[]int{1, 100, 1},
		kafka.NewKafkaDownloader(cfg.concurrency, cfg.groupid, receiveTopics, receiveMseeages, viper.GetString("mqaddr")),
	)
	kafkaDownloader.Connect(streamer.NewDisjunctions(kafkaDownloader, 100))

	//02-00 Combiner
	combiner := actor.NewActor(
		"combiner",
		broker,
		[]string{
			actor.MsgAppHash,
			actor.MsgReapingCompleted,
		},
		[]string{
			actor.MsgClearCommand,
		},
		[]int{1},
		storage.NewCombiner(cfg.concurrency, cfg.groupid, actor.MsgClearCommand),
	)
	combiner.Connect(streamer.NewConjunctions(combiner))

	//02 aggre
	aggreSelector := actor.NewActor(
		"aggreSelector",
		broker,
		[]string{
			actor.MsgReceiptHashList,
			actor.MsgClearCommand,
			actor.MsgInclusive},
		[]string{
			actor.MsgSelectedReceipts,
			actor.MsgReapingCompleted,
		},
		[]int{1, 1},
		workers.NewAggreSelector(cfg.concurrency, cfg.groupid),
	)
	aggreSelector.Connect(streamer.NewDisjunctions(aggreSelector, 1))

	//03 calculateRoothash
	calculateRoothash := actor.NewActor(
		"calculateRoothash",
		broker,
		[]string{
			actor.MsgSelectedReceipts,
			actor.MsgInclusive},
		[]string{
			actor.MsgRcptHash,
			actor.MsgGasUsed,
		},
		[]int{1, 1},
		workers.NewCalculateRoothash(cfg.concurrency, cfg.groupid),
	)
	calculateRoothash.Connect(streamer.NewConjunctions(calculateRoothash))
	relations := map[string]string{}
	relations[actor.MsgRcptHash] = viper.GetString("msgexch")
	relations[actor.MsgGasUsed] = viper.GetString("msgexch")
	//04 kafkaUploader
	kafkaUploader := actor.NewActor(
		"kafkaUploader",
		broker,
		[]string{
			actor.MsgRcptHash,
			actor.MsgGasUsed,
		},
		[]string{},
		[]int{},
		kafka.NewKafkaUploader(cfg.concurrency, cfg.groupid, relations, viper.GetString("mqaddr")),
	)
	kafkaUploader.Connect(streamer.NewDisjunctions(kafkaUploader, 4))

	//starter
	selfStarter := streamer.NewDefaultProducer("selfStarter", []string{actor.MsgStarting}, []int{1})
	broker.RegisterProducer(selfStarter)
	broker.Serve()

	//start signel
	streamerStarting := actor.Message{
		Name:   actor.MsgStarting,
		Height: 0,
		Round:  0,
		Data:   "start",
	}
	broker.Send(actor.MsgStarting, &streamerStarting)
}

func (cfg *Config) Stop() {

}
