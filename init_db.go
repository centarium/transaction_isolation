package main

import (
	"fmt"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/spf13/cobra"
)

var initDbCmd = &cobra.Command{
	Use:   "init_db",
	Short: "Init database",
	RunE:  InitDBCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(initDbCmd)
}

func InitDBCmd(_ *cobra.Command, args []string) (err error) {
	db, err := GetInitConnection()
	if err != nil {
		fmt.Printf("failed to connect db server: %s", err)
	}

	if _, err = db.Exec(dbDropString()); err != nil {
		fmt.Printf("failed exec drop db script: %s", err)
	}

	if _, err = db.Exec(dbInitString()); err != nil {
		fmt.Printf("failed exec init db script: %s", err)
	}

	return nil
}

func dbDropString() string {
	return `DROP DATABASE IF EXISTS transaction_isolation;`
}

func dbInitString() string {
	return `CREATE DATABASE transaction_isolation`
}
