package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/centarium/transaction_isolation/tests"
	"github.com/spf13/cobra"
	"strconv"
)

var mysqlLostUpdateHack = &cobra.Command{
	Use:   "mysql_lost_update_hack <account_id> <amount> <version>",
	Short: "Mysql lost update hack demonstration",
	RunE:  MysqlLostUpdateHack,
}

// Command init function.
func init() {
	rootCmd.AddCommand(mysqlLostUpdateHack)
}

func MysqlLostUpdateHack(_ *cobra.Command, args []string) (err error) {
	if len(args) != 3 {
		return errors.New("Required 3 args: 1) accountID, 2) Amount, 3) Version ")
	}
	dbName := "mysql"
	db, err := GetDbConnection(dbName)
	if err != nil {
		fmt.Printf("failed to connect db server: %s", err)
		return
	}

	ctx := context.Background()
	txLevel := sql.LevelRepeatableRead
	accountID, _ := strconv.Atoi(args[0])
	amount, _ := strconv.Atoi(args[1])
	version, _ := strconv.Atoi(args[2])

	if err = tests.MySQLLostUpdateHack(ctx, db, txLevel, dbName, accountID, amount, version); err != nil {
		fmt.Printf("MySQLLostUpdateHack error: %s", err)
	}
	return err
}
