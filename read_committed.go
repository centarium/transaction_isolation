package main

import (
	"context"
	"database/sql"
	"fmt"
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
	Use:   "read_committed",
	Short: "Read committed demonstration",
	RunE:  ReadCommittedCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(readCommitted)
}

func ReadCommittedCmd(_ *cobra.Command, args []string) (err error) {
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

	txLevel := sql.LevelReadCommitted

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

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	/*
		if err = TestLostUpdateBetweenTransactionAndBasic(ctx, db, txLevel); err != nil {
			fmt.Printf("TestLostUpdateBetweenTransactionAndBasic error: %s", err)
			return
		}

		if err = DropAndCreateInvoice(db); err != nil {
			fmt.Printf("DropAndCreateInvoice error: %s", err)
			return
		}*/

	//first transaction will have committed, than second transaction will commit
	if err = TestLostUpdateBetweenTransactionAndTransactionAtomicUpdate(ctx, db, txLevel); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionAtomicUpdate error: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestSerializationAnomaly(ctx, db, txLevel); err != nil {
		fmt.Printf("TestSerializationAnomaly error: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate(ctx, db, txLevel); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate error: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestSkewedWrite(ctx, db, txLevel); err != nil {
		fmt.Printf("TestSkewedWrite error: %s", err)
		return
	}

	return
}
