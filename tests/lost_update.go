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

func TestLostUpdate(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Lost Update-----------------")

	defer func() {
		if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
			fmt.Printf("DropAndCreateInvoice error: %s", err)
		}
	}()

	//print current invoice sum
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
		if invoiceSum, err = tx1.GetAmount(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 150)

		//update invoice in transaction 1
		if err = tx1.UpdateInvoice(invoiceSum+500, false); err != nil {
			return err
		}
		//time.Sleep(time.Millisecond * 100)

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

		var invoiceSum int64
		if invoiceSum, err = tx2.GetAmount(); err != nil {
			return err
		}

		time.Sleep(time.Millisecond * 100)

		//update invoice in transaction 2
		if err = tx2.UpdateInvoice(invoiceSum+200, false); err != nil {
			return err
		}

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}

	//print current invoice sum
	if err = helper.PrintAmount(db); err != nil {
		return
	}

	return
}
