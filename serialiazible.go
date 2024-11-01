package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
	"time"
)

// full serialization - consequential perform of transactions
// 2PL serialization - exclusive write lock, shared read lock
// SSI - snapshot serialize isolation. Internal DB mechanism traces conflicting transaction with
// transaction artefacts like phantom read, dirty read e.t.c and prevents them by termination/interruption one of transaction
var serializationCmd = &cobra.Command{
	Use:   "serializable",
	Short: "Snapshot serialization demonstration(In Postgres, instead full serialization and 2PL serialization)",
	RunE:  SerializableCmd,
}

// Command init function.
func init() {
	rootCmd.AddCommand(serializationCmd)
}

func SerializableCmd(_ *cobra.Command, args []string) (err error) {
	dbName := GetDbName(args)
	db, err := CreateAccounts(dbName)
	if err != nil {
		fmt.Printf("failed to create accounts: %s", err)
	}

	ctx := context.Background()
	txLevel := sql.LevelSerializable

	/*
		mysql: Lock wait timeout exceeded
		postgres: commit
		sqlserver: lock
		oracle: commit
	*/
	if err = tests.TestSerializableSelectPlusUpdateLocks(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSerializableSelectPlusUpdateLocks error: %s \n", err)
	}

	childCtx, cancelFunc := context.WithTimeout(ctx, time.Second*3)
	defer func() {
		cancelFunc()
	}()
	//mysql: Lock wait timeout exceeded
	//postgres: 1000
	//sqlserver: lock
	//oracle: 1000
	if err = tests.TestDirtyRead(childCtx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestDirtyRead error: %s \n", err)
	}

	//mysql: Lock
	//postgres: 1000
	//sqlserver: lock
	//oracle: 1000
	if err = tests.TestLostUpdate(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate error: %s", err)
	}

	//mysql: deadlock found
	//postgres: could not serialize access
	//sqlserver: lock
	//oracle: 1000, 1000
	if err = tests.TestPhantom(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestPhantom error: %s", err)
	}

	//mysql: 1000, 1000
	//postgres: 1000, 1000
	//sqlserver: 1000, 1000
	//oracle: 1000, tx1: 1000, tx2 commit, then tx1 commit
	if err = tests.TestNonRepeatableRead(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("Serializable TestNonRepeatableRead error: %s", err)
	}

	/*
		postgres: 1000, ERROR: could not serialize access due to read/write dependencies among transactions (SQLSTATE 40001)
		mysql: 1000
		oracle: 0
		sqlserver: 1000
	*/
	if err = tests.TestSkewedWriteWithdrawal(ctx, db, txLevel, dbName); err != nil {
		fmt.Printf("TestSkewedWriteWithdrawal error: %s", err)
	}

	return
}
