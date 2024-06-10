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

	//create invoice for tests
	if _, err = db.Exec(fmt.Sprintf(
		"INSERT  into invoices(id, name, amount) VALUES (%d, 'test_1', 1000)", InvoiceIdInt,
	)); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	return nil
}