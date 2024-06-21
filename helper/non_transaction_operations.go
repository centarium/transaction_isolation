package helper

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

func PrintAmount(db *sqlx.DB) (err error) {
	row := db.QueryRow(`Select amount from invoices WHERE id = ` + InvoiceIdStr)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice: %s\n", err)
		return
	}

	var invoiceSum int64
	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice: %s\n", err)
		return
	}

	fmt.Printf("Invoice sum: %d \n", invoiceSum)
	return
}

func DropAndCreateInvoice(db *sqlx.DB, dbName string) (err error) {
	if err = TruncateInvoices(db, dbName); err != nil {
		return err
	}

	//create invoice for tests
	if err = CreateInvoice(db, InvoiceIdInt); err != nil {
		return err
	}

	return nil
}

func TruncateInvoices(db *sqlx.DB, dbName string) (err error) {
	truncateString := `TRUNCATE invoices`
	switch dbName {
	case "sqlserver":
		truncateString = `TRUNCATE table invoices`
	case "oracle":
		truncateString = `TRUNCATE table SYSTEM.invoices`
	}

	if _, err = db.Exec(truncateString); err != nil {
		fmt.Printf("failed exec drop test invoice: %s", err)
		return
	}
	return nil
}

func CreateInvoice(db *sqlx.DB, invoiceId int) (err error) {
	if _, err = db.Exec(fmt.Sprintf(
		"INSERT  into invoices(id, user_id, amount) VALUES (%d, 1, 1000)", invoiceId,
	)); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}
	return nil
}

func PrintUserInvoicesSum(db *sqlx.DB, userId int) (err error) {
	row := db.QueryRow(fmt.Sprintf(`Select SUM(amount) from invoices WHERE user_id = %d`, userId))

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice: %s\n", err)
		return
	}

	var invoiceSum int64
	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice: %s\n", err)
		return
	}

	fmt.Printf("User invoices sum: %d \n", invoiceSum)
	return
}

func Withdrawal(db *sqlx.DB, invoiceId int, dbName string) (err error) {
	if dbName == "mysql" {
		querySetTotalAmount := `
			SELECT SUM(amount) INTO @total_amount FROM transaction_isolation.invoices WHERE user_id = 1;
		`
		if _, err = db.Exec(querySetTotalAmount); err != nil {
			fmt.Printf("failed set @total_amount: %s \n", err)
		}
		queryUpdate := fmt.Sprintf(`
			UPDATE invoices
			SET amount = amount - 1000
			WHERE id = %d AND amount >= 1000 AND @total_amount >= 2000
		`, invoiceId)
		if _, err = db.Exec(queryUpdate); err != nil {
			fmt.Printf("failed update amount: %s \n", err)
		}

		return
	}

	query := fmt.Sprintf(`UPDATE invoices SET amount = amount-1000 WHERE id = %d AND amount >=1000 AND  (
      			SELECT SUM(amount) FROM invoices WHERE user_id = 1
		) >= 2000`, invoiceId)

	if _, err = db.Exec(query); err != nil {
		fmt.Printf("failed exec withdrawal %s \n", err)
	}
	return
}
