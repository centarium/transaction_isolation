package tests

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/helper"
	"github.com/jmoiron/sqlx"
)

// TestDirtyRead - update not committed transaction - outside transaction queries can see this update
func TestDirtyRead(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, dbName string) (err error) {
	fmt.Println("----------------Dirty read -----------------")

	defer func() {
		if err = helper.DropAndCreateAccount(db, dbName); err != nil {
			fmt.Printf("DropAndCreateAccount error: %s \n", err)
		}
	}()

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
		tx2.Rollback()
	}()

	//print account amount in tx1: 1000
	if err = tx1.PrintAmount(); err != nil {
		return
	}

	//change account amount in tx2: 1000 -> 1500
	if err = tx2.UpdateAccount(1500); err != nil {
		return
	}

	//print account amount in tx1 again
	if err = tx1.PrintAmount(); err != nil {
		return
	}

	return
}
