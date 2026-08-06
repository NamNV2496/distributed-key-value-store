package main

import (
	"os"

	"github.com/namnv2496/go-redis-raft/cmd/dashboard"
	"github.com/namnv2496/go-redis-raft/cmd/proxy"
	"github.com/namnv2496/go-redis-raft/cmd/redis"
	"github.com/spf13/cobra"
)

func main() {
	if err := execute(); err != nil {
		os.Exit(1)
	}
}
func execute() error {
	rootCmd := &cobra.Command{
		Use:   "raft-redis",
		Short: "A simple redis using raft and multiple IO",
	}
	rootCmd.AddCommand(redis.Cmd)
	rootCmd.AddCommand(proxy.Cmd)
	rootCmd.AddCommand(dashboard.Cmd)
	return rootCmd.Execute()
}
