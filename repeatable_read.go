package main

import (
	"context"
	"database/sql"
	"fmt"
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
	//mysql: 1200
	//oracle: error - isolation level is not supported
	/*if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdate error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}*/

	/*
		postgres: 1000, tx1: 1000, tx2 commit, then tx1 commit
		mysql: 1000, tx1: 1000, tx2 commit, then tx1 commit
		sqlserver: 1000, tx1: 1000, tx1 commit, then tx2 commit(!)
		oracle: error - isolation level not supported
	*/
	if err = tests.NotRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NotRepeatableRead error: %s", err)
		return
	}

	/*
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
