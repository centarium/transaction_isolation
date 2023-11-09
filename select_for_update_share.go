package main

import (
	"context"
	"database/sql"
	"fmt"
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
	db, err := GetDbConnection()
	//db.SetConnMaxIdleTime(time.Millisecond * 1000)
	//db.SetConnMaxLifetime(time.Millisecond * 1000)
	//db.SetMaxOpenConns(3)
	//db.SetMaxIdleConns(3)
	if err != nil {
		fmt.Printf("failed to connect db server: %s", err)
		return
	}

	if _, err = db.Exec(`Drop Table if exists invoices;`); err != nil {
		fmt.Printf("failed exec drop invoices: %s", err)
		return
	}

	//create table invoices
	createInvoicesString := `CREATE TABLE invoices(
    id bigint primary key,
    name text NOT NULL,
    amount bigint,
    created_at timestamp default now(),
    updated_at timestamp default now()
)`

	if _, err = db.Exec(createInvoicesString); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Millisecond*6000)
	defer cancelFunc()

	txLevel := sql.LevelReadCommitted

	if err = TestShareLocks(ctx, db, txLevel); err != nil {
		fmt.Printf("TestShareLocks error: %s", err)
		err = nil
	}

	if err = DropAndCreateInvoice(db); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}

	if err = TestUpdateLocks(ctx, db, txLevel); err != nil {
		fmt.Printf("TestUpdateLocks error: %s", err)
		err = nil
	}

	return
}
