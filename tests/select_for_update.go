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

func TestSelectForUpdate(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Select For Update to prevent lost update-----------------")

	defer func() {
		if err = helper.DropAndCreateAccount(db, dbName); err != nil {
			fmt.Printf("DropAndCreateAccount error: %s", err)
		}
	}()

	//print current account sum
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

		var accountSum int64
		//lock for update
		if accountSum, err = tx1.GetAmountWithExclusiveLock(); err != nil {
			return err
		}

		time.Sleep(time.Millisecond * 150)
		//update account in transaction 1
		if err = tx1.UpdateAccount(accountSum + 500); err != nil {
			return err
		}

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
		var accountSum int64
		if accountSum, err = tx2.GetAmountWithExclusiveLock(); err != nil {
			return err
		}

		//update account in transaction 2
		if err = tx2.UpdateAccount(accountSum + 200); err != nil {
			return err
		}

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}

	//print current account sum
	//must be 1700
	if err = helper.PrintAmount(db); err != nil {
		return
	}

	return
}
