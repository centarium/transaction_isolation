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

// TestUncommittedNotRepeatableRead - start transaction, read invoice,
// then update invoice outside of transaction - transaction can see changes.
func NotRepeatableRead(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Not repeatable read-----------------")

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
		//print amount in tx1 before update amount
		if err = tx1.PrintAmount(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 300)
		//print amount after update in tx2 outside transaction
		if err = helper.PrintAmount(db); err != nil {
			return err
		}
		//print amount after update in tx2 in tx1
		if err = tx1.PrintAmount(); err != nil {
			return err
		}

		if err = tx1.UpdateInvoice(1800, false); err != nil {
			return err
		}

		//sql server - wait for commit
		time.Sleep(time.Second * 1)

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

		if err = tx2.PrintAmount(); err != nil {
			return err
		}

		//update invoice in transaction 2
		if err = tx2.UpdateInvoice(1500, false); err != nil {
			return err
		}

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}
	return nil
}
