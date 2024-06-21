package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
)

func TestSerializationAnomaly(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------TestSerializationAnomaly-----------------")
	//prepare data
	if _, err = db.Exec(`INSERT  into invoices(id, name, amount) 
			VALUES (2, 'test_1', 2000),
			       (3, 'test_1', 1000),
			       (4, 'test_1', 1000),
			    	(5, 'test_2', 3000),
			        (6, 'test_2', 4000)`); err != nil {
		fmt.Printf("failed exec create new invoice: %s", err)
		return
	}

	//create transaction 1
	var tx1 *sqlx.Tx
	if tx1, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction 1 created")

	//create transaction 2
	var tx2 *sqlx.Tx
	if tx2, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction 2 created")

	//sum(test1) = 5000, sum(test2) = 7000
	if _, err = tx1.Exec(`Insert into invoices (id, amount, name) 
    		values (7, (SELECT sum(amount) FROM invoices WHERE name = 'test_1'  ),'test_2' )`); err != nil {
		fmt.Printf("failed exec update invoice: %s", err)
		return
	}

	//sum(test1) = 5000, sum(test2) = 12000
	if _, err = tx2.Exec(`Insert into invoices (id, amount, name) 
    		values (8, (SELECT sum(amount) FROM invoices WHERE name = 'test_2'  ), 'test_1') `); err != nil {
		fmt.Printf("failed exec update invoice: %s", err)
		return
	}
	//sum(test1) = 17000(?) 12000(?) , sum(test2) = 12000

	if err = tx1.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	if err = tx2.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	//select and print current invoice sum
	var (
		row           *sql.Row
		invoiceAmount int
	)
	if row = db.QueryRow(`Select sum(amount) from invoices WHERE name = 'test_1' `); err != nil {
		fmt.Printf("failed select invoices: %s", err)
		return
	}

	if err = row.Scan(&invoiceAmount); err != nil {
		fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
		return
	}

	fmt.Printf("Test1 sum(must be 17000): %d \n", invoiceAmount)

	if row = db.QueryRow(`Select sum(amount) from invoices WHERE name = 'test_2' `); err != nil {
		fmt.Printf("failed select invoices: %s", err)
		return
	}

	if err = row.Scan(&invoiceAmount); err != nil {
		fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
		return
	}

	fmt.Printf("Test2 sum(must be 12000): %d \n", invoiceAmount)

	return
}
