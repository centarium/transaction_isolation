package helper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
)

const (
	InvoiceIdStr = "1"
	InvoiceIdInt = 1
)

type Transaction struct {
	tx             *sqlx.Tx
	transactionNum int
	isCommitted    bool
	isRollbacked   bool
	dbName         string
}

const DB = "transaction_isolation"

func (t *Transaction) PrintInvoicesSumByUserID(userID int) (err error) {
	row := t.tx.QueryRow(fmt.Sprintf(`Select sum(amount) from invoices WHERE user_id = '%d'`, userID))

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	var invoicesSum int64
	if err = row.Scan(&invoicesSum); err != nil {
		fmt.Printf("failed to scan invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	fmt.Printf("Invoice sum in transaction %d: %d \n", t.transactionNum, invoicesSum)
	return
}

func (t *Transaction) PrintAmount() (err error) {
	row := t.tx.QueryRow(`Select amount from invoices WHERE id = ` + InvoiceIdStr)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	var invoiceSum int64
	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	fmt.Printf("Invoice sum in transaction %d: %d \n", t.transactionNum, invoiceSum)
	return
}

func (t *Transaction) GetAmountWithShareLock() (invoiceSum int64, err error) {
	query := fmt.Sprintf("Select amount from invoices WHERE id = %d FOR SHARE", InvoiceIdInt)
	if t.dbName == "sqlserver" {
		query = fmt.Sprintf("Select amount from invoices WITH (HOLDLOCK, ROWLOCK) WHERE id = %d", InvoiceIdInt)
	}

	row := t.tx.QueryRow(query)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	return
}

func (t *Transaction) GetAmountWithExclusiveLock() (invoiceSum int64, err error) {
	query := fmt.Sprintf("Select amount from invoices WHERE id = %d FOR UPDATE", InvoiceIdInt)
	if t.dbName == "sqlserver" {
		query = fmt.Sprintf("Select amount from invoices WITH (UPDLOCK) WHERE id = %d", InvoiceIdInt)
	}

	row := t.tx.QueryRow(query)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	return
}

func (t *Transaction) GetAmount() (invoiceSum int64, err error) {
	row := t.tx.QueryRow(`Select amount from invoices WHERE id = ` + InvoiceIdStr)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	return
}

func (t *Transaction) Withdrawal(invoiceId int, dbName string) (err error) {
	if dbName == "mysql" {
		querySetTotalAmount := `
			SELECT SUM(amount) INTO @total_amount FROM transaction_isolation.invoices WHERE user_id = 1;
		`
		if _, err = t.tx.Exec(querySetTotalAmount); err != nil {
			fmt.Printf("failed set @total_amount in transaction %d: %s \n", t.transactionNum, err)
		}
		queryUpdate := fmt.Sprintf(`
			UPDATE invoices
			SET amount = amount - 1000
			WHERE id = %d AND amount >= 1000 AND @total_amount >= 2000
		`, invoiceId)
		if _, err = t.tx.Exec(queryUpdate); err != nil {
			fmt.Printf("failed update amount in transaction %d: %s \n", t.transactionNum, err)
		}

		return
	}

	query := fmt.Sprintf(`UPDATE invoices SET amount = amount-1000 WHERE id = %d AND amount >=1000 AND  (
      			SELECT SUM(amount) FROM invoices WHERE user_id = 1
		) >= 2000`, invoiceId)

	if _, err = t.tx.Exec(query); err != nil {
		fmt.Printf("failed exec withdrawal in transaction %d: %s \n", t.transactionNum, err)
	}
	return
}

func (t *Transaction) UpdateInvoiceId(newAmount int64, isIncrement bool) (err error) {
	fmt.Printf("Update invoice amount in transaction %d to %d \n", t.transactionNum, newAmount)

	queryLeftSide := `UPDATE invoices SET amount = `
	if isIncrement {
		queryLeftSide += `amount + `
	}

	var query string
	switch t.dbName {
	case "postgres":
		query = queryLeftSide + ` $1 WHERE id = $2`
	case "mysql":
		query = queryLeftSide + ` ? WHERE id = ?`
	case "sqlserver":
		query = queryLeftSide + ` @Amount WHERE id = @InvoiceID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("InvoiceID", 2)); err != nil {
			fmt.Printf("failed exec update invoice in transaction %d: %s \n", t.transactionNum, err)
		}
		return
	case "oracle":
		query = queryLeftSide + ` :Amount WHERE id = :InvoiceID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("InvoiceID", 2)); err != nil {
			fmt.Printf("failed exec update invoice in transaction %d: %s \n", t.transactionNum, err)
		}
		return
	default:
		query = `UPDATE invoices SET amount = $1 Where id = $2`
	}

	if _, err = t.tx.Exec(query, newAmount, 2); err != nil {
		fmt.Printf("failed exec update invoice in transaction %d: %s \n", t.transactionNum, err)
		return
	}
	return
}

func (t *Transaction) UpdateInvoice(newAmount int64, isIncrement bool) (err error) {
	fmt.Printf("Update invoice amount in transaction %d to %d \n", t.transactionNum, newAmount)

	queryLeftSide := `UPDATE invoices SET amount = `
	if isIncrement {
		queryLeftSide += `amount + `
	}

	var query string
	switch t.dbName {
	case "postgres":
		query = queryLeftSide + ` $1 WHERE id = $2`
	case "mysql":
		query = queryLeftSide + ` ? WHERE id = ?`
	case "sqlserver":
		query = queryLeftSide + ` @Amount WHERE id = @InvoiceID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("InvoiceID", InvoiceIdInt)); err != nil {
			fmt.Printf("failed exec update invoice in transaction %d: %s \n", t.transactionNum, err)
		}
		return
	case "oracle":
		query = queryLeftSide + ` :Amount WHERE id = :InvoiceID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("InvoiceID", InvoiceIdInt)); err != nil {
			fmt.Printf("failed exec update invoice in transaction %d: %s \n", t.transactionNum, err)
		}
		return
	default:
		query = `UPDATE invoices SET amount = $1 Where id = $2`
	}

	if _, err = t.tx.Exec(query, newAmount, InvoiceIdInt); err != nil {
		fmt.Printf("failed exec update invoice in transaction %d: %s \n", t.transactionNum, err)
		return
	}
	return
}

func CreateTransaction(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel, transactionNum int, dbName string) (t *Transaction, err error) {
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
	}); err != nil {
		fmt.Printf("failed to create transaction %d: %s\n", transactionNum, err)
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
		fmt.Printf("error to commit transaction %d: %s\n", t.transactionNum, err)
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
		fmt.Printf("error to rollback transaction %d: %s\n", t.transactionNum, err)
		return
	}

	fmt.Printf("Transaction %d rollbacked \n", t.transactionNum)
	return
}
