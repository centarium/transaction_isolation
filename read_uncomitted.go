package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/helper"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

var readUncomittedIsolationCmd = &cobra.Command{
	Use:   "read_uncommitted <database>",
	Short: "Read uncommitted demonstration",
	RunE:  ReadUncommittedIsolationCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(readUncomittedIsolationCmd)
}

func ReadUncommittedIsolationCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateInvoices(dbName)
	if err != nil {
		fmt.Printf("failed to create invoices: %s", err)
		return
	}

	ctx := context.Background()

	txLevel := sql.LevelReadUncommitted

	//mysql: 1500
	//postgres: 1000 - as read committed
	//sqlserver: 1500
	//oracle: - error - not supported
	if err = tests.DirtyRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("DirtyRead error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	//mysql: Deadlock found
	//postgres: deadlock detected
	//sqlserver: Transaction was deadlocked
	//oracle: - error - not supported
	if err = tests.ShareLocks(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("ShareLocks error: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	return
}
