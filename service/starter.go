package service

import (
	cmn "github.com/HPISTechnologies/3rd-party/tm/common"
	"github.com/HPISTechnologies/component-lib/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start generic-hashing service Daemon",
	RunE:  startCmd,
}

func init() {
	flags := StartCmd.Flags()

	flags.String("mqaddr", "localhost:9092", "host:port of kafka ")
	flags.String("mqaddr2", "localhost:9092", "host:port of kafka ")

	flags.String("exec-rcpt-hash", "exec-rcpt-hash", "topic for received merged rcpts")

	flags.String("msgexch", "msgexch", "topic for msg exchange")

	flags.String("inclusive-txs", "inclusive-txs", "topic for receive txlist")

	flags.Int("concurrency", 4, "num of threads")

	flags.String("log", "log", "topic for send log")

	flags.String("logcfg", "./log.toml", "log conf path")

	flags.Uint64("svcid", 7, "service id of generic-hashing,range 1 to 255")
	flags.Uint64("insid", 1, "instance id of generic-hashing,range 1 to 255")

	flags.Bool("draw", false, "draw flow graph")
	flags.Int("nidx", 0, "node index in cluster")
	flags.String("nname", "node1", "node name in cluster")
}

func startCmd(cmd *cobra.Command, args []string) error {
	log.InitLog("generic.log", viper.GetString("logcfg"), "generic-hashing", viper.GetString("nname"), viper.GetInt("nidx"))

	en := NewConfig()
	en.Start()

	// Wait forever
	cmn.TrapSignal(func() {
		// Cleanup
		en.Stop()
	})

	return nil
}
