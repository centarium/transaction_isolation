package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
)

var repeatableReadCmd = &cobra.Command{
	Use:   "repeatable_read",
	Short: "Repeatable read demonstration(based on MVCC - in mysql, postgresql)",
	RunE:  RepeatableReadCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(repeatableReadCmd)
}

func RepeatableReadCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateAccounts(dbName)
	if err != nil {
		fmt.Printf("failed to create accounts: %s", err)
		return
	}

	ctx := context.Background()

	//oracle: error - isolation level is not supported
	txLevel := sql.LevelRepeatableRead

	//sqlserver: error - Transaction (Process ID 52) was deadlocked on lock resources with another
	//process and has been chosen as the deadlock victim.  Rerun the transaction
	//postgres: error - ERROR: could not serialize access due to concurrent update
	//mysql: 1500 - lost update
	if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdate error: %s", err)
	}

	/*
		postgres: 1000, tx1: 1000, tx2 commit, then tx1 commit
		mysql: 1000, tx1: 1000, tx2 commit, then tx1 commit
		sqlserver: 1000, tx1: 1000, tx1 commit, then tx2 commit(!)
	*/
	if err = tests.TestNonRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestNonRepeatableRead error: %s", err)
	}
	/*
		postgres: 1000, tx1: 1000, tx2 commit, then tx1 commit
		mysql: 1000, tx1: 1000, tx2 commit, then tx1 commit
		sqlserver: 1000, tx1: 1000, tx1 commit, then tx2 commit(!)
	*/
	if err = tests.TestNonRepeatableReadDelete(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestNonRepeatableRead error: %s", err)
	}

	/*
		postgres: 1000, 1000
		mysql: 1000, 1000
		sqlserver: 1000, 2000
	*/
	if err = tests.TestPhantom(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestPhantom error: %s", err)
	}

	/*
		postgres: 0
		mysql: 0
		sqlserver: 1000
	*/
	if err = tests.TestSkewedWriteWithdrawal(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSkewedWriteWithdrawal error: %s", err)
	}

	if dbName == "mysql" {
		//mysql: 0
		if err = tests.TestSkewedWriteWithdrawal2(ctx, db, txLevel, dbName); err != nil {
			fmt.Printf("TestSkewedWriteWithdrawal2 error: %s", err)
		}
	}

	return
}
