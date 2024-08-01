package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

// read committed prevents dirty read and dirty write isolation artefacts
// dirty read - transaction two read changes of not yet ended transaction one,
// then transaction one interrupts, transaction two continue work with non-actual data
// dirty write - provide atomic state for sequential update one record by multiple transaction
// for example, Alice and Bob want to buy one car. This leads to compete queries in two tables -
// invoices and car_owners. Correct situation - Alice was first, she became a car owner and get
// an invoice. Bob couldn't buy car. Incorrect situation due to race condition - Alice was first
// in invoices table, but second in car_owners table - leads to situation when Bob become car owner,
// but invoice was sent to Alice
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
	if err = tests.DirtyRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("DirtyRead error: %s \n", err)
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
	if err = tests.NonRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NonRepeatableRead error: %s \n", err)
	}

	/*
		postgres: 1000, 2000
		mysql: 1000, 2000
		sqlserver: 1000, 2000
		oracle: 1000, 2000
	*/
	if err = tests.TestPhantom(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NonRepeatableRead error: %s \n", err)
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

	/*
		postgres: 1000
		mysql: 1000
		oracle: 1000
		sqlserver: 1000
	*/
	if err = tests.TestWithdrawal(db, dbName); err != nil {
		fmt.Printf("TestWithdrawal error: %s", err)
	}

	return
}
