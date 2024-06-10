package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"time"
)

func TestShareLocks1(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------TestShareLock-----------------")

	//create transaction 1
	var tx1 *sqlx.Tx
	if tx1, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction 1 created")

	//create transaction 2
	var tx2 *sqlx.Tx
	if tx2, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction 2 created")

	fmt.Println("Lock 1 FOR SHARE")
	row := tx1.QueryRow(`SELECT id FROM invoices Where id = 1 FOR SHARE `)

	if err = row.Err(); err != nil {
		fmt.Printf("Failed to select 1: %s", err)
		return
	}

	var id int
	if err = row.Scan(&id); err != nil {
		fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
		return
	}

	fmt.Printf("Test1 id: %d \n", id)
	time.Sleep(time.Second * 60)

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		time.Sleep(time.Millisecond * 500)
		fmt.Println("Lock 1 FOR UPDATE")
		if _, err = tx1.Exec(`UPDATE invoices SET name = 'test_11' WHERE id = 1`); err != nil {
			fmt.Printf("failed exec update invoice: %s", err)
			return err
		}

		if err = tx1.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}
		fmt.Println("Transaction 1 committed")

		return nil
	})

	group.Go(func() error {
		fmt.Println("Lock 2 FOR SHARE")
		row = tx2.QueryRow(`SELECT id FROM invoices Where id = 1 FOR SHARE`)

		if err = row.Err(); err != nil {
			fmt.Printf("Failed to select 2: %s", err)
			return err
		}

		if err = row.Scan(&id); err != nil {
			fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
			return err
		}

		fmt.Printf("Test1 id: %d \n", id)

		fmt.Println("Lock 2 FOR UPDATE")
		if _, err = tx2.Exec(`UPDATE invoices SET name = 'test_1' WHERE id = 1`); err != nil {
			fmt.Printf("failed exec update invoice: %s", err)
			return err
		}

		if err = tx2.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}

		fmt.Println("Transaction 2 committed")

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	return
}

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
	if err = tests.TestSelectForUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("DirtyRead error: %s", err)
		return
	}

	/*
		postgres: 1000, 1500
		mysql: 1000, 1500
		sqlserver: 1000, 1500
		oracle: 1000, 1500
	*/
	/*if err = tests.NotRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestUncommittedNotRepeatableRead error: %s", err)
		return
	}*/

	/*if err = TestShareLocks1(ctx, db, txLevel); err != nil {
		fmt.Println("TestShareLocks err + " + err.Error())
	}*.

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
