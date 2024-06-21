package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

// full serialization - consequential perform of transactions
// 2PL serialization - exclusive write lock, shared read lock
// SSI - snapshot serialize isolation. Internal DB mechanism traces conflicting transaction with
// transaction artefacts like phantom read, dirty read e.t.c and prevents them by termination/interruption one of transaction
var serializationCmd = &cobra.Command{
	Use:   "serialization",
	Short: "Snapshot serialization demonstration(In Postgres, instead full serialization and 2PL serialization)",
	RunE:  SerializationCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(serializationCmd)
}

func SerializationCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateInvoices(dbName)
	if err != nil {
		fmt.Printf("failed to create invoices: %s", err)
		return
	}

	ctx := context.Background()

	txLevel := sql.LevelSerializable

	if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate error: %s", err)
		return
	}

	//oracle: 1000, 1000
	/*if err = tests.TestPhantomRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NotRepeatableRead error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}*/

	//oracle: 1000, tx1: 1000, tx2 commit, then tx1 commit
	/*if err = tests.NotRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NotRepeatableRead error: %s", err)
		return
	}*/

	/*
		postgres: 1000, ERROR: could not serialize access due to read/write dependencies among transactions (SQLSTATE 40001)
		mysql: 1000
		oracle: 0
		sqlserver: 1000
	*/
	/*if err = tests.TestSkewedWriteWithdrawal(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSkewedWriteWithdrawal error: %s", err)
		return
	}*/

	/*if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
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

	return
}
