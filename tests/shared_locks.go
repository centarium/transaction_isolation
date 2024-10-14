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

// TestSharedLocks test share lock deadlock
func TestSharedLocks(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Shared locks -----------------")

	defer func() {
		if errRefill := helper.DropAndCreateInvoice(db, dbName); errRefill != nil {
			fmt.Printf("DropAndCreateInvoice error: %s", errRefill)
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

		if _, err = tx1.GetAmountWithShareLock(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 100)

		if err = tx1.UpdateInvoice(1500); err != nil {
			return err
		}
		return err
	})

	group.Go(func() error {
		var tx2 *helper.Transaction
		if tx2, err = helper.CreateTransaction(ctx, db, txLevel, 2, dbName); err != nil {
			return err
		}
		defer func() {
			tx2.Close(err)
		}()

		if _, err = tx2.GetAmountWithShareLock(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 100)

		if err = tx2.UpdateInvoice(1500); err != nil {
			return err
		}
		return err
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}

	return
}

// TestSerializableSelectPlusUpdateLocks test serializable shared deadlock
func TestSerializableSelectPlusUpdateLocks(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Serializable Shared locks -----------------")

	defer func() {
		if errRefill := helper.DropAndCreateInvoice(db, dbName); errRefill != nil {
			fmt.Printf("DropAndCreateInvoice error: %s", errRefill)
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

		if err = tx1.PrintAmount(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 100)

		if err = tx1.UpdateInvoice(1500); err != nil {
			return err
		}
		return err
	})

	group.Go(func() error {
		var tx2 *helper.Transaction
		if tx2, err = helper.CreateTransaction(ctx, db, txLevel, 2, dbName); err != nil {
			return err
		}
		defer func() {
			tx2.Close(err)
		}()

		if err = tx2.PrintAmount(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 100)

		if err = tx2.UpdateInvoice(1500); err != nil {
			return err
		}
		return err
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}

	return
}
