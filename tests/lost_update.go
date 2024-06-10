package tests

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/helper"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
	"time"
)

/*
func TestLostUpdateBetweenTransactionAndTransactionAtomicUpdate(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Lost Update between Transaction And Transaction Atomic Update-----------------")

	var tx1 *helper.Transaction
	if tx1, err = helper.CreateTransaction(ctx, db, txLevel, 1, dbName); err != nil {
		return
	}
	defer func() {
		tx1.Close(err)
	}()

	var tx2 *helper.Transaction
	if tx2, err = helper.CreateTransaction(ctx, db, txLevel, 2, dbName); err != nil {
		return
	}
	defer func() {
		tx2.Close(err)
	}()

	//print current invoice sum
	if err = helper.PrintAmount(db); err != nil {
		return
	}

	//increment invoice amount in tx1: 1000 + 500
	if err = tx1.UpdateInvoice(500, true); err != nil {
		return
	}

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		//commit tx1 after update in tx2
		time.Sleep(time.Millisecond * 400)
		tx1.Commit()
		return nil
	})

	group.Go(func() error {
		//change invoice in 2 transaction
		if err = tx2.UpdateInvoice(200, true); err != nil {
			return err
		}
		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	tx2.Commit()

	//must be 1700
	if err = helper.PrintAmount(db); err != nil {
		return
	}

	return
}*/

func TestSelectForUpdate(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Select For Update to prevent lost update-----------------")

	//print current invoice sum
	//1000
	if err = helper.PrintAmount(db); err != nil {
		return
	}

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {

		var tx1 *helper.Transaction
		if tx1, err = helper.CreateTransaction(ctx, db, txLevel, 1, dbName); err != nil {
			return err
		}
		defer func() {
			tx1.Close(err)
		}()

		var invoiceSum int64
		//lock for update
		if invoiceSum, err = tx1.GetAmountWithExclusiveLock(); err != nil {
			return err
		}

		time.Sleep(time.Millisecond * 150)
		//update invoice in transaction 1
		if err = tx1.UpdateInvoice(invoiceSum+500, false); err != nil {
			return err
		}

		tx1.Commit()

		return nil
	})

	group.Go(func() error {
		var tx2 *helper.Transaction
		if tx2, err = helper.CreateTransaction(ctx, db, txLevel, 2, dbName); err != nil {
			return err
		}
		defer func() {
			tx2.Close(err)
		}()

		time.Sleep(time.Millisecond * 100)
		var invoiceSum int64
		if invoiceSum, err = tx2.GetAmountWithExclusiveLock(); err != nil {
			return err
		}

		//update invoice in transaction 2
		if err = tx2.UpdateInvoice(invoiceSum+200, false); err != nil {
			return err
		}

		tx2.Commit()

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	//print current invoice sum
	//must be 1700
	if err = helper.PrintAmount(db); err != nil {
		return
	}

	return
}

func TestLostUpdate(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Lost Update-----------------")

	var tx1 *helper.Transaction
	if tx1, err = helper.CreateTransaction(ctx, db, txLevel, 1, dbName); err != nil {
		return
	}
	defer func() {
		tx1.Close(err)
	}()

	var tx2 *helper.Transaction
	if tx2, err = helper.CreateTransaction(ctx, db, txLevel, 2, dbName); err != nil {
		return
	}
	defer func() {
		tx2.Close(err)
	}()

	//print current invoice sum
	if err = helper.PrintAmount(db); err != nil {
		return
	}

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {

		var invoiceSum int64
		if invoiceSum, err = tx1.GetAmount(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 100)

		//update invoice in transaction 1
		if err = tx1.UpdateInvoice(invoiceSum+500, false); err != nil {
			return err
		}

		tx1.Commit()

		return nil
	})

	group.Go(func() error {
		var invoiceSum int64
		if invoiceSum, err = tx2.GetAmount(); err != nil {
			return err
		}

		time.Sleep(time.Millisecond * 100)

		//update invoice in transaction 2
		if err = tx2.UpdateInvoice(invoiceSum+200, false); err != nil {
			return err
		}

		tx2.Commit()

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	//print current invoice sum
	//must NOT be 1700
	if err = helper.PrintAmount(db); err != nil {
		return
	}

	return
}
