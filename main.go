package main

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"log"
)

var (
	//2 parameters - isolation level, database
	rootCmd = &cobra.Command{
		Use:     "<isolation_level> <database>",
		Example: "read_committed postgres",
		//<database>: "postgres", "mysql", "oracle", "sqlserver"
		ValidArgs: []string{"read_uncommitted <database>", "read_committed <database>",
			"snapshot_isolation <database>", "serializable <database>"},
	}
)

func main() {
	var err error
	if err = Execute(); err != nil {
		log.Println(err.Error())
	}

}

func Execute() (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Printf("service exited with error: %v", err)
		}
	}()

	if err = rootCmd.Execute(); err != nil && err != context.Canceled {
		panic(err)
	}

	return nil
}
