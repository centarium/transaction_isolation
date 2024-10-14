package main

import (
	"fmt"
	"github.com/centarium/transaction_isolation/helper"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/spf13/cobra"
)

var initDbCmd = &cobra.Command{
	Use:       "init_db <database>",
	Example:   "init_db postgres",
	Short:     "Init database",
	RunE:      InitDBCmd,
	ValidArgs: []string{"postgres", "mysql", "sqlserver", "oracle"},
}

// Command init function.
func init() {
	rootCmd.AddCommand(initDbCmd)
}

const DB = "transaction_isolation"

func GetInitPostgresConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("pgx", "postgres://admin:1234@localhost:5432/postgres?connect_timeout=5&sslmode=disable&search_path=public")
	return
}

func GetInitMySQLConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("mysql", "root:MySQL123@(localhost:3306)/mysql")
	return
}

func GetDBMysqlConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("mysql", "root:MySQL123@(localhost:3306)/"+DB)
	return
}

func GetInitSQLServerConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("sqlserver", "sqlserver://sa:admin857GH@localhost:1433/master")
	return
}

func GetDBSQLServerConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("sqlserver", "sqlserver://sa:admin857GH@localhost:1433/"+DB)
	return
}

func GetDBPostgresConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("pgx", "postgres://admin:1234@localhost:5432/"+DB+"?connect_timeout=5&sslmode=disable&search_path=public")
	return
}

func GetInitOracleConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("godror", "SYSTEM/admin857GH@localhost:1521/oracle")
	return
}

func GetDBOracleConnection() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect("godror", "SYSTEM/admin857GH@localhost:1521/oracle")
	return
}

func GetDbName(args []string) string {
	if len(args) == 0 {
		return "postgres"
	}
	return args[0]
}

func GetDbConnection(dbName string) (db *sqlx.DB, err error) {
	switch dbName {
	case "postgres":
		db, err = GetDBPostgresConnection()
	case "mysql":
		db, err = GetDBMysqlConnection()
	case "sqlserver":
		db, err = GetDBSQLServerConnection()
	case "oracle":
		db, err = GetDBOracleConnection()
	default:
		db, err = GetDBPostgresConnection()
	}
	return
}

func InitDBCmd(_ *cobra.Command, args []string) (err error) {
	var db *sqlx.DB
	dbName := GetDbName(args)
	fmt.Println("DB name:" + dbName)
	switch dbName {
	case "postgres":
		db, err = GetInitPostgresConnection()
	case "mysql":
		db, err = GetInitMySQLConnection()
	case "sqlserver":
		db, err = GetInitSQLServerConnection()
	case "oracle":
		db, err = GetInitOracleConnection()
	default:
		db, err = GetInitPostgresConnection()
	}

	if err != nil {
		fmt.Printf("failed to connect db server: %s", err)
		return
	}

	if dbName != "oracle" {
		if _, err = db.Exec(dbDropString()); err != nil {
			fmt.Printf("failed exec drop db script: %s", err)
			return
		}
	}

	if _, err = db.Exec(dbInitString()); err != nil {
		fmt.Printf("failed exec init db script: %s", err)
		return
	}

	return nil
}

func dbDropString() string {
	return `DROP DATABASE IF EXISTS ` + DB + ";"
}

func dbInitString() string {
	return `CREATE DATABASE ` + DB + ";"
}

func getCreateInvoicesTableString(dbName string) string {
	postgresQuery := `CREATE TABLE invoices(
    	id bigint primary key,
    	user_id bigint NOT NULL,
    	amount bigint,
    	created_at timestamp default now(),
    	updated_at timestamp default now()
	)`

	fmt.Println("DB name:" + dbName)
	switch dbName {
	case "postgres":
		return postgresQuery
	case "mysql":
		return `CREATE TABLE invoices(
    	id bigint primary key,
    	user_id bigint NOT NULL,
    	amount bigint,
    	created_at timestamp default now(),
    	updated_at timestamp default now()
	) ENGINE = InnoDB`
	case "oracle":
		return `CREATE TABLE invoices (
                          id NUMBER PRIMARY KEY,
                          user_id NUMBER,
                          amount NUMBER,
                          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	case "sqlserver":
		return `CREATE TABLE invoices(
                         id bigint primary key,
                         user_id bigint NOT NULL,
                         amount bigint,
                         created_at datetime default CURRENT_TIMESTAMP,
                         updated_at datetime default CURRENT_TIMESTAMP )`
	}
	fmt.Println(" -> unknown db, switch to postgres ")
	return postgresQuery
}

func CreateInvoices(dbName string) (db *sqlx.DB, err error) {
	db, err = GetDbConnection(dbName)
	if err != nil {
		fmt.Printf("failed to connect db server: %s", err)
		return
	}

	dropTableQuery := `Drop Table if exists invoices`
	if dbName == "oracle" {
		dropTableQuery = "Drop Table invoices"
	}
	if _, err = db.Exec(dropTableQuery); err != nil {
		fmt.Printf("failed exec drop invoices: %s", err)
		return
	}

	//create table invoices
	createInvoicesString := getCreateInvoicesTableString(dbName)

	if _, err = db.Exec(createInvoicesString); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	if err = helper.DropAndCreateInvoice(db, dbName); err != nil {
		fmt.Printf("DropAndCreateInvoice error: %s", err)
		return
	}
	return
}
