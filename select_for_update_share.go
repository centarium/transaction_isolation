package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/helper"
	"github.com/spf13/cobra"
	"time"
)

var selectUpdateShareCmd = &cobra.Command{
	Use:   "updateAndShare",
	Short: "Comparative test, share and update locking",
	RunE:  SelectUpdateShareCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(selectUpdateShareCmd)
}

func SelectUpdateShareCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateInvoices(dbName)
	if err != nil {
		fmt.Printf("failed to create invoices: %s", err)
		return
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Millisecond*6000)
	defer cancelFunc()

	txLevel := sql.LevelReadCommitted

	if err = TestShareLocks(ctx, db, txLevel); err != nil {
		fmt.Printf("TestShareLocks error: %s", err)
		err = nil
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestUpdateLocks(ctx, db, txLevel); err != nil {
		fmt.Printf("TestUpdateLocks error: %s", err)
		err = nil
	}

	return
}
