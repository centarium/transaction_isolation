package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/helper"
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

	if err = TestUncommittedNotRepeatableRead(ctx, db, txLevel); err != nil {
		fmt.Printf("TestUncommittedNotRepeatableRead error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestUncommittedDirtyReadByBasicQuery(ctx, db, txLevel); err != nil {
		fmt.Printf("TestUncommittedDirtyReadByBasicQuery error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestUncommittedDirtyReadByAnotherTransaction(ctx, db, txLevel); err != nil {
		fmt.Printf("TestUncommittedDirtyReadByAnotherTransaction error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestPhantomReadBetweenTransactionAndBasic(ctx, db, txLevel); err != nil {
		fmt.Printf("TestPhantomReadBetweenTransactionAndBasic error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestPhantomReadBetweenTransactionAndTransaction(ctx, db, txLevel); err != nil {
		fmt.Printf("TestPhantomReadBetweenTransactionAndTransaction error: %s", err)
		return
	}

	/*
		if err = DropAndCreateInvoice(db); err != nil {
			fmt.Printf("DropAndCreateInvoice error: %s", err)
			return
		}

		if err = TestLostUpdateBetweenTransactionAndBasic(ctx, db, txLevel); err != nil {
			fmt.Printf("TestLostUpdateBetweenTransactionAndBasic error: %s", err)
			return
		}*/

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestLostUpdateBetweenTransactionAndTransactionAtomicUpdate(ctx, db, txLevel); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionAtomicUpdate error: %s", err)
		err = nil
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestSerializationAnomaly(ctx, db, txLevel); err != nil {
		fmt.Printf("TestSerializationAnomaly error: %s", err)
		return
	}

	return
}
