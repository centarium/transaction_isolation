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
	Use:   "serializable",
	Short: "Snapshot serialization demonstration(In Postgres, instead full serialization and 2PL serialization)",
	RunE:  SerializableCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(serializationCmd)
}

func SerializableCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateInvoices(dbName)
	if err != nil {
		fmt.Printf("failed to create invoices: %s", err)
	}

	ctx := context.Background()

	txLevel := sql.LevelSerializable

	//mysql: 1000
	//postgres: 1000
	//sqlserver: block(without READ_COMMITTED_SNAPSHOT  ON;)
	//oracle: 1000
	if err = tests.DirtyRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("DirtyRead error: %s \n", err)
	}

	if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate error: %s", err)
	}

	//oracle: 1000, 1000
	if err = tests.TestPhantom(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestPhantom error: %s", err)
	}

	//oracle: 1000, tx1: 1000, tx2 commit, then tx1 commit
	if err = tests.NonRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("Serializable NonRepeatableRead error: %s", err)
	}

	/*
		postgres: 1000, ERROR: could not serialize access due to read/write dependencies among transactions (SQLSTATE 40001)
		mysql: 1000
		oracle: 0
		sqlserver: 1000
	*/
	if err = tests.TestSkewedWriteWithdrawal(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSkewedWriteWithdrawal error: %s", err)
	}

	return
}
