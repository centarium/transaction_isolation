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

// TestNonRepeatableRead - start transaction, read account,
// then update account outside of transaction - transaction can see changes.
func TestNonRepeatableRead(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Nonrepeatable read-----------------")

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
		//print amount in tx1 before update amount in tx2
		if err = tx1.PrintAmount(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 300)
		//print amount after update in tx2 in tx1
		if err = tx1.PrintAmount(); err != nil {
			return err
		}
		//sql server - wait for commit
		time.Sleep(time.Millisecond * 500)

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
		time.Sleep(time.Millisecond * 100)

		//update account in transaction 2
		if err = tx2.UpdateAccount(1500); err != nil {
			return err
		}

		fmt.Println("Account updated")

		return err
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}
	return
}

// TestNonRepeatableRead - start transaction, read account,
// then update account outside of transaction - transaction can see changes.
func TestNonRepeatableReadDelete(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Nonrepeatable read(Delete operation)-----------------")

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
		//print amount in tx1 before update amount in tx2
		if err = tx1.PrintAmount(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 300)
		//print amount after update in tx2 in tx1
		if err = tx1.PrintAmount(); err != nil {
			return err
		}
		//sql server - wait for commit
		time.Sleep(time.Millisecond * 500)

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
		time.Sleep(time.Millisecond * 100)

		//update account in transaction 2
		if err = tx2.DeleteAccount(1500); err != nil {
			return err
		}

		fmt.Println("Account deleted")

		return err
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}
	return
}
