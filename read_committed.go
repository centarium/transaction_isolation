package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

var readCommitted = &cobra.Command{
	Use:   "read_committed <database>",
	Short: "Read committed demonstration",
	RunE:  ReadCommittedCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(readCommitted)
}

func ReadCommittedCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateInvoices(dbName)
	if err != nil {
		fmt.Printf("failed to create invoices: %s", err)
		return
	}

	ctx := context.Background()

	txLevel := sql.LevelReadCommitted

	//mysql: 1000
	//postgres: 1000
	//sqlserver: block(without READ_COMMITTED_SNAPSHOT  ON;)
	//oracle: 1000
	if err = tests.TestDirtyRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestDirtyRead error: %s \n", err)
	}

	//mysql: 1500
	//postgres: 1500
	//sqlserver: 1500
	//oracle: 1500
	if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdate error: %s \n", err)
	}

	//mysql: 1500
	//postgres: 1500
	//sqlserver: 1500
	//oracle: 1500
	if err = tests.TestSelectForUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSelectForUpdate error: %s \n", err)
	}

	/*
		postgres: 1000, 1500
		mysql: 1000, 1500
		sqlserver: 1000, 1500
		oracle: 1000, 1500
	*/
	if err = tests.TestNonRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestNonRepeatableRead error: %s \n", err)
	}

	/*
		postgres: 1000, 2000
		mysql: 1000, 2000
		sqlserver: 1000, 2000
		oracle: 1000, 2000
	*/
	if err = tests.TestPhantom(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestNonRepeatableRead error: %s \n", err)
		return
	}

	/*
		postgres: 0
		mysql: 0
		oracle: 0
		sqlserver: 1000
	*/
	if err = tests.TestSkewedWriteWithdrawal(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSkewedWriteWithdrawal error: %s \n", err)
	}

	return
}
