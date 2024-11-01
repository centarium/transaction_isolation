package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

var snapshotIsolationCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Snapshot Isolation Level demonstration(based on MVCC in sql server)",
	RunE:  SnapshotCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(snapshotIsolationCmd)
}

func SnapshotCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateAccounts(dbName)
	if err != nil {
		fmt.Printf("failed to create accounts: %s", err)
	}

	ctx := context.Background()

	txLevel := sql.LevelSnapshot

	if _, err = db.Exec("ALTER DATABASE " + DB + " SET ALLOW_SNAPSHOT_ISOLATION ON"); err != nil {
		fmt.Printf("Snapshot isolation on error: %s", err)
		return
	}

	//sqlserver: Snapshot isolation transaction aborted due to update conflict
	if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdate error: %s", err)
	}

	//	sqlserver: 1000, tx1: 1000, tx2 commit, then tx1 commit
	if err = tests.TestNonRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestNonRepeatableRead error: %s", err)
	}

	//	sqlserver: 1000, tx1: 1000, tx2 commit, then tx1 commit
	if err = tests.TestNonRepeatableReadDelete(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestNonRepeatableRead error: %s", err)
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
