package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

var snapShotIsolationCmd = &cobra.Command{
	Use:   "snapshot_isolation",
	Short: "Snapshot Isolation demonstration(also repeatable read, also MVCC - multi version control)",
	RunE:  SnapshotIsolationCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(snapShotIsolationCmd)
}

func SnapshotIsolationCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateInvoices(dbName)
	if err != nil {
		fmt.Printf("failed to create invoices: %s", err)
		return
	}

	ctx := context.Background()

	txLevel := sql.LevelRepeatableRead

	//sqlserver: error - deadlock, transaction1 rollbacked
	//postgres: error - transaction2 rollbacked
	//mysql: 1200
	//oracle: error - isolation level is not supported
	/*if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}*/

	/*
		postgres: 1500, tx1: 1000
		mysql: 1500, tx1: 1000
		sqlserver: 1000, tx1: 1000, tx2 not committed, deadlock
		oracle: error - isolation level not supported
	*/
	if err = tests.NotRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NotRepeatableRead error: %s", err)
		return
	}

	/*
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

		if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
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

	//При апдейте первая транзакция закомиттит результат, затем вторая транзакция завершится с ошибкой
	//чтобы не допустить аномалии потерянный апдейт, т.е.
	//i:100 t1-> 100 -> 100 + 30 = 130
	// 		t2-> 100 -> 100 + 50 = error
	// Транзакция завершается с ошибкой из - за конфликтующих требований.
	// С одной стороны, транзакции в данном режиме не должны видеть результаты друг друга,
	// с другой - при одновременном апдейте в двух транзакциях одной и той же строки возникает потерянный апдейт
	/*if err = TestLostUpdateBetweenTransactionAndTransactionAtomicUpdate(ctx, db, txLevel); err != nil {
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

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate(ctx, db, txLevel); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestSkewedWrite(ctx, db, txLevel); err != nil {
		fmt.Printf("TestSkewedWrite error: %s", err)
		return
	}
	*/
	return
}
