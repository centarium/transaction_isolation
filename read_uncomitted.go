package main

import (
	"context"
	"database/sql"
	"fmt"
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
	if err = tests.TestDirtyRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestDirtyRead error: %s \n", err)
	}

	//mysql: Deadlock found
	//postgres: deadlock detected
	//sqlserver: Transaction was deadlocked
	//oracle: - error - not supported
	if err = tests.TestSharedLocks(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSharedLocks error: %s \n", err)
		err = nil
	}

	return
}
