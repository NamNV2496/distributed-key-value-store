package main

import (
	"github.com/namnv2496/go-redis-raft/cmd"
	"github.com/spf13/cobra"
)

func main() {
	if err := Execute(); err != nil {
		panic(err)
	}
}

func Execute() error {
	rootCmd := &cobra.Command{
		Short: "A simple mock tool",
	}
	rootCmd.AddCommand(cmd.RedisCmd)
	return rootCmd.Execute()
}
