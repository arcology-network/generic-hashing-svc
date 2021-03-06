package main

import (
	"os"

	tmCli "github.com/arcology-network/3rd-party/tm/cli"
	"github.com/arcology-network/generic-hashing-svc/service"
)

func main() {
	st := service.StartCmd

	cmd := tmCli.PrepareMainCmd(st, "BC", os.ExpandEnv("$HOME/monacos/omtree"))
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}

}
