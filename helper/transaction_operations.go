package helper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type Transaction struct {
	tx             *sqlx.Tx
	transactionNum int
	isCommitted    bool
	isRollbacked   bool
	dbName         string
}

func (t *Transaction) PrintAmount() (err error) {
	row := t.tx.QueryRow(`Select amount from invoices WHERE id = 1`)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice by transaction %d: %s", t.transactionNum, err)
		return
	}

	var invoiceSum int64
	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice by transaction %d: %s", t.transactionNum, err)
		return
	}

	fmt.Printf("Invoice sum in transaction %d: %d \n", t.transactionNum, invoiceSum)
	return
}

func (t *Transaction) GetAmount() (invoiceSum int64, err error) {
	row := t.tx.QueryRow(`Select amount from invoices WHERE id = 1`)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice by transaction %d: %s", t.transactionNum, err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice by transaction %d: %s", t.transactionNum, err)
		return
	}

	fmt.Printf("Invoice sum in transaction %d: %d \n", t.transactionNum, invoiceSum)

	return
}

func (t *Transaction) UpdateInvoice(newAmount int) (err error) {
	fmt.Printf("Update invoice amount in transaction %d to %d \n", t.transactionNum, newAmount)

	var query string
	switch t.dbName {
	case "postgres":
		query = `UPDATE invoices SET amount = $1 WHERE id = $2`
	case "mysql":
		query = `UPDATE invoices SET amount = ? WHERE id = ?`
	case "sqlserver":
		query = `UPDATE invoices SET amount = @Amount WHERE id = @InvoiceID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("InvoiceID", 1)); err != nil {
			fmt.Printf("failed exec create invoice1 in transaction %d: %s", t.transactionNum, err)
		}
		return
	case "oracle":
		query = `UPDATE invoices SET amount = $1 Where id = $2`
	default:
		query = `UPDATE invoices SET amount = $1 Where id = $2`
	}

	if _, err = t.tx.Exec(query, newAmount, 1); err != nil {
		fmt.Printf("failed exec create invoices in transaction %d: %s", t.transactionNum, err)
		return
	}
	return
}

func CreateTransaction(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, transactionNum int, dbName string) (t *Transaction, err error) {
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
	}); err != nil {
		fmt.Printf("failed to create transaction %d: %s", transactionNum, err)
		return
	} else {
		fmt.Printf("Transaction %d created \n", transactionNum)
	}

	return &Transaction{
		tx:             tx,
		transactionNum: transactionNum,
		dbName:         dbName,
	}, nil
}

func (t *Transaction) Close(err error) {
	if err != nil {
		t.Rollback()
	} else {
		t.Commit()
	}
}

func (t *Transaction) Commit() {
	if t.isCommitted || t.isRollbacked {
		return
	}
	if err := t.tx.Commit(); err != nil {
		fmt.Printf("error to commit transaction %d: %s", t.transactionNum, err)
		return
	}

	fmt.Printf("Transaction %d committed \n", t.transactionNum)
	t.isCommitted = true
	return
}

func (t *Transaction) Rollback() {
	if t.isCommitted || t.isRollbacked {
		return
	}
	if err := t.tx.Rollback(); err != nil {
		fmt.Printf("error to rollback transaction %d: %s", t.transactionNum, err)
		return
	}

	fmt.Printf("Transaction %d rollbacked \n", t.transactionNum)
	return
}
