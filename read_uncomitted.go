package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/spf13/cobra"
)

var readUncomittedIsolationCmd = &cobra.Command{
	Use:   "read_uncommitted",
	Short: "",
	RunE:  ReadUncommittedIsolationCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(readUncomittedIsolationCmd)
}

func ReadUncommittedIsolationCmd(_ *cobra.Command, args []string) (err error) {
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

	txLevel := sql.LevelReadUncommitted

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

	if err = TestExclusiveBlockAndReadOutsideTransaction(ctx, db, txLevel); err != nil {
		fmt.Printf("TestExclusiveBlockAndReadOutsideTransaction error: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestShareBlockAndUpdateOutsideTransaction(ctx, db, txLevel); err != nil {
		fmt.Printf("TestShareBlockAndUpdateOutsideTransaction error: %s", err)
		return
	}

	//lock
	/*
		if err = DropAndCreateInvoice(db); err != nil {
			fmt.Printf("DropAndCreateInvoice error: %s", err)
			return
		}

		if err = TestLostUpdateBetweenTransactionAndBasic(ctx, db, txLevel); err != nil {
			fmt.Printf("TestLostUpdateBetweenTransactionAndBasic error: %s", err)
			return
		}*/

	return
}
