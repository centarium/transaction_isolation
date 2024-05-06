package helper

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

func PrintAmount(db *sqlx.DB) (err error) {
	row := db.QueryRow(`Select amount from invoices WHERE id = 1`)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select invoice: %s", err)
		return
	}

	var invoiceSum int64
	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum: %d \n", invoiceSum)
	return
}
