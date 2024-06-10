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
	/*if err = tests.DirtyRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("DirtyRead error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}*/

	//mysql: 1500
	//postgres: 1500
	//sqlserver: 1500
	//oracle: 1500
	/*if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}*/

	//mysql: 1500
	//postgres: 1500
	//sqlserver: 1500
	//oracle: 1500
	/*if err = tests.TestSelectForUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("DirtyRead error: %s", err)
		return
	}*/

	/*
		postgres: 1000, 1500
		mysql: 1000, 1500
		sqlserver: 1000, 1500
		oracle: 1000, 1500
	*/
	if err = tests.NotRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestUncommittedNotRepeatableRead error: %s", err)
		return
	}

	/*
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

	/*
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
		}*/

	return
}
