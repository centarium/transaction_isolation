package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/helper"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

var repeatableReadCmd = &cobra.Command{
	Use:   "repeatable_read",
	Short: "Repeatable read demonstration(based on MVCC - in mysql, postgresql)",
	RunE:  RepeatableReadCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(repeatableReadCmd)
}

func RepeatableReadCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateInvoices(dbName)
	if err != nil {
		fmt.Printf("failed to create invoices: %s", err)
		return
	}

	ctx := context.Background()

	txLevel := sql.LevelRepeatableRead

	//sqlserver: error - Transaction (Process ID 52) was deadlocked on lock resources with another
	//process and has been chosen as the deadlock victim.  Rerun the transaction
	//postgres: error - ERROR: could not serialize access due to concurrent update
	//mysql: 1500 - lost update
	//oracle: error - isolation level is not supported
	if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdate error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	/*
		postgres: 1000, tx1: 1000, tx2 commit, then tx1 commit
		mysql: 1000, tx1: 1000, tx2 commit, then tx1 commit
		sqlserver: 1000, tx1: 1000, tx1 commit, then tx2 commit(!)
		oracle: error - isolation level not supported
	*/
	/*if err = tests.NotRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NotRepeatableRead error: %s", err)
		return
	}*/

	/*
		postgres: 1000, 1000
		mysql: 1000, 1000
		sqlserver: 1000, 2000
		oracle: isolation level not supported
	*/
	/*if err = tests.TestPhantomRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NotRepeatableRead error: %s", err)
		return
	}*/

	/*
		postgres: 0
		mysql: 0
		oracle: 0
		sqlserver: 1000
	*/
	/*if err = tests.TestSkewedWriteWithdrawal(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSkewedWriteWithdrawal error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}*/

	/*
		postgres: 1000
		mysql: 1000
		oracle: 1000
		sqlserver: 1000
	*/
	/*if err = tests.TestWithdrawal(db, dbName); err != nil {
		fmt.Printf("TestWithdrawal error: %s", err)
		return
	}*/

	/*
		if err = TestSerializationAnomaly(ctx, db, txLevel); err != nil {
			fmt.Printf("TestSerializationAnomaly error: %s", err)
			return
		}
	*/
	return
}
