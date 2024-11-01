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

func TestSkewedWriteWithdrawal(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Skewed Write Withdrawal-----------------")

	defer func() {
		if err = helper.DropAndCreateAccount(db, dbName); err != nil {
			fmt.Printf("DropAndCreateAccount error: %s", err)
		}
	}()

	if err = helper.CreateAccount(db, 2); err != nil {
		return err
	}
	defer func() {
		helper.TruncateAccounts(db, dbName)
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
		err = tx1.Withdrawal(1, dbName)
		time.Sleep(100 * time.Millisecond)

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
		err = tx2.Withdrawal(2, dbName)
		time.Sleep(100 * time.Millisecond)

		return err
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}

	return helper.PrintUserAccountsSum(db, 1)
}

func TestSkewedWriteWithdrawal2(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Skewed Write Withdrawal 2 Mysql-----------------")

	defer func() {
		if err = helper.DropAndCreateAccount(db, dbName); err != nil {
			fmt.Printf("DropAndCreateAccount error: %s", err)
		}
	}()

	if err = helper.CreateAccount(db, 2); err != nil {
		return err
	}
	defer func() {
		helper.TruncateAccounts(db, dbName)
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
		err = tx1.Withdrawal2(1, dbName)
		time.Sleep(100 * time.Millisecond)

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
		err = tx2.Withdrawal2(2, dbName)
		time.Sleep(100 * time.Millisecond)

		return err
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s\n", err)
		return
	}

	return helper.PrintUserAccountsSum(db, 1)
}
