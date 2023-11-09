package main

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"log"
)

var (
	rootCmd = &cobra.Command{
		Use: "Transaction Isolation",
	}
)

func GetInitConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("pgx", "postgres://admin:1234@localhost:5432/postgres?connect_timeout=5&sslmode=disable&search_path=public")
	return
}

func GetDbConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("pgx", "postgres://admin:1234@localhost:5432/transaction_isolation?connect_timeout=5&sslmode=disable&search_path=public")
	return
}

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
