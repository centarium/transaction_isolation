package main

import (
	"context"
	"database/sql"
	"fmt"
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
	db, err := GetDbConnection()
	if err != nil {
		fmt.Printf("failed to connect db server: %s", err)
		return
	}

	if _, err = db.Exec(`Drop Table if exists invoices;`); err != nil {
		fmt.Printf("failed exec drop invoices: %s", err)
		return
	}

	//create table invoices
	createInvoicesString := `CREATE TABLE invoices(
    id bigint primary key,
    name text NOT NULL,
    amount bigint,
    created_at timestamp default now(),
    updated_at timestamp default now()
)`

	if _, err = db.Exec(createInvoicesString); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	ctx := context.Background()

	txLevel := sql.LevelSerializable

	if err = TestUncommittedNotRepeatableRead(ctx, db, txLevel); err != nil {
		fmt.Printf("TestUncommittedNotRepeatableRead error: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestUncommittedDirtyReadByBasicQuery(ctx, db, txLevel); err != nil {
		fmt.Printf("TestUncommittedDirtyReadByBasicQuery error: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestUncommittedDirtyReadByAnotherTransaction(ctx, db, txLevel); err != nil {
		fmt.Printf("TestUncommittedDirtyReadByAnotherTransaction error: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestPhantomReadBetweenTransactionAndBasic(ctx, db, txLevel); err != nil {
		fmt.Printf("TestPhantomReadBetweenTransactionAndBasic error: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
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

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestLostUpdateBetweenTransactionAndTransaction(ctx, db, txLevel); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransaction error: %s", err)
		err = nil
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestSerializationAnomaly(ctx, db, txLevel); err != nil {
		fmt.Printf("TestSerializationAnomaly error: %s", err)
		return
	}

	return
}
