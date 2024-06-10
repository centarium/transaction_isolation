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

// ShareLocks test share lock deadlock
func ShareLocks(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Share locks -----------------")

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		var tx1 *helper.Transaction
		if tx1, err = helper.CreateTransaction(ctx, db, txLevel, 1, dbName); err != nil {
			return
		}
		defer func() {
			tx1.Close(err)
		}()

		if _, err = tx1.GetAmountWithShareLock(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 100)
		if err = tx1.UpdateInvoice(1500, false); err != nil {
			return err
		}
		return err
	})

	group.Go(func() error {
		var tx2 *helper.Transaction
		if tx2, err = helper.CreateTransaction(ctx, db, txLevel, 2, dbName); err != nil {
			return
		}
		defer func() {
			tx2.Close(err)
		}()

		if _, err = tx2.GetAmountWithShareLock(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 100)
		//change invoice amount in tx2: 1000 -> 1500
		if err = tx2.UpdateInvoice(1500, false); err != nil {
			return err
		}
		return err
	})

	return
}
