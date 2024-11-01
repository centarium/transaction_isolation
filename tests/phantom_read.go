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

func TestPhantom(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Phantom-----------------")

	defer func() {
		if err = helper.DropAndCreateAccount(db, dbName); err != nil {
			fmt.Printf("DropAndCreateAccount error: %s", err)
		}
	}()

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		var tx1 *helper.Transaction
		if tx1, err = helper.CreateTransaction(ctx, db, txLevel, 1, dbName); err != nil {
			return err
		}
		defer func() {
			tx1.Close(err)
		}()

		if err = tx1.PrintAccountsSumByUserID(1); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 200)
		if err = tx1.PrintAccountsSumByUserID(1); err != nil {
			return err
		}

		return nil
	})

	group.Go(func() error {

		time.Sleep(time.Millisecond * 100)
		if err = helper.CreateAccount(db, 2); err != nil {
			return err
		}
		fmt.Println("New account added")

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}

	return
}
