package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

var snapshotIsolationCmd = &cobra.Command{
	Use:   "snapshot_isolation",
	Short: "Snapshot Isolation demonstration(based on MVCC - in sql server)",
	RunE:  SnapshotIsolationCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(snapshotIsolationCmd)
}

func SnapshotIsolationCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateInvoices(dbName)
	if err != nil {
		fmt.Printf("failed to create invoices: %s", err)
	}

	ctx := context.Background()

	txLevel := sql.LevelSnapshot

	//sqlserver: Snapshot isolation transaction aborted due to update conflict
	if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdate error: %s", err)
	}

	//	sqlserver: 1000, tx1: 1000, tx2 commit, then tx1 commit
	if err = tests.NonRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("NonRepeatableRead error: %s", err)
	}

	//	sqlserver: 1000, 1000
	if err = tests.TestPhantom(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestPhantom error: %s", err)
	}

	//	sqlserver: 0
	if err = tests.TestSkewedWriteWithdrawal(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSkewedWriteWithdrawal error: %s", err)
	}

	return
}
