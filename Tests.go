package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
	"time"
)

func DropAndCreateInvoice(db *sqlx.DB) (err error) {
	if _, err = db.Exec(`TRUNCATE invoices`); err != nil {
		fmt.Printf("failed exec drop test invoice: %s", err)
		return
	}

	//create invoice for tests
	if _, err = db.Exec(`INSERT  into invoices(id, name, amount) VALUES (1, 'test_1', 1000)`); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	return nil
}

func TestSkewedWrite(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------TestSkewedWrite-----------------")
	//create table invoices
	createDoctorsString := `CREATE TABLE doctors(
    shift_id bigint,
    name text NOT NULL,
    on_call bool
)`
	if _, err = db.Exec(createDoctorsString); err != nil {
		fmt.Printf("Failed to create doctors: %s", err)
		return
	}

	initDoctorsString := `INSERT INTO doctors(shift_id, name, on_call) VALUES
                                                 (123,'Alice',true),
                                                 (123, 'Bob',true)`

	if _, err = db.Exec(initDoctorsString); err != nil {
		fmt.Printf("Failed to init doctors: %s", err)
		return
	}

	defer func() {
		if _, err = db.Exec(`Drop table doctors`); err != nil {
			fmt.Printf("Failed to drop doctors: %s", err)
			return
		}
	}()

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

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {

		//Alice
		row := tx1.QueryRow(`SELECT count(*) FROM doctors Where shift_id = 123 and on_call = true`)

		if err = row.Err(); err != nil {
			fmt.Printf("Failed to select 1: %s", err)
			return err
		}

		var count int
		if err = row.Scan(&count); err != nil {
			fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
			return err
		}

		if count > 1 {
			if _, err = tx1.Exec(`UPDATE doctors SET on_call = false WHERE shift_id = 123 AND name = 'Alice' `); err != nil {
				fmt.Printf("failed exec update invoice: %s", err)
				return err
			}
		}

		if err = tx1.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}
		fmt.Println("Transaction 1 committed")

		return nil
	})

	group.Go(func() error {

		//Bob
		row := tx2.QueryRow(`SELECT count(*) FROM doctors Where shift_id = 123 and on_call = true`)

		if err = row.Err(); err != nil {
			fmt.Printf("Failed to select 1: %s", err)
			return err
		}

		var count int
		if err = row.Scan(&count); err != nil {
			fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
			return err
		}

		if count > 1 {
			if _, err = tx2.Exec(`UPDATE doctors SET on_call = false WHERE shift_id = 123 AND name = 'Bob' `); err != nil {
				fmt.Printf("failed exec update invoice: %s", err)
				return err
			}
		}

		if err = tx2.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}
		fmt.Println("Transaction 2 committed")

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	var countOnCall int
	row := db.QueryRow(`Select count(*) from doctors WHERE shift_id = 123 AND on_call = true`)

	if err = row.Scan(&countOnCall); err != nil {
		fmt.Printf("failed to scan 1 countOnCall: %s", err)
		return
	}

	fmt.Printf("Count on call: %d \n", countOnCall)

	return
}

func TestShareLocks(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------TestShareLock-----------------")

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

	fmt.Println("Lock 1 FOR SHARE")
	row := tx1.QueryRow(`SELECT id FROM invoices Where id = 1 FOR SHARE`)

	if err = row.Err(); err != nil {
		fmt.Printf("Failed to select 1: %s", err)
		return
	}

	var id int
	if err = row.Scan(&id); err != nil {
		fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
		return
	}

	fmt.Printf("Test1 id: %d \n", id)

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		time.Sleep(time.Millisecond * 500)
		fmt.Println("Lock 1 FOR UPDATE")
		if _, err = tx1.Exec(`UPDATE invoices SET name = 'test_11' WHERE id = 1`); err != nil {
			fmt.Printf("failed exec update invoice: %s", err)
			return err
		}

		if err = tx1.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}
		fmt.Println("Transaction 1 committed")

		return nil
	})

	group.Go(func() error {
		fmt.Println("Lock 2 FOR SHARE")
		row = tx2.QueryRow(`SELECT id FROM invoices Where id = 1 FOR SHARE`)

		if err = row.Err(); err != nil {
			fmt.Printf("Failed to select 2: %s", err)
			return err
		}

		if err = row.Scan(&id); err != nil {
			fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
			return err
		}

		fmt.Printf("Test1 id: %d \n", id)

		fmt.Println("Lock 2 FOR UPDATE")
		if _, err = tx2.Exec(`UPDATE invoices SET name = 'test_1' WHERE id = 1`); err != nil {
			fmt.Printf("failed exec update invoice: %s", err)
			return err
		}

		if err = tx2.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}

		fmt.Println("Transaction 2 committed")

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	return
}

func TestUpdateLocks(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------TestUpdateLock-----------------")

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

	fmt.Println("Lock 1 Select FOR UPDATE")

	row := tx1.QueryRow(`SELECT id FROM invoices Where id = 1 FOR UPDATE`)

	if err = row.Err(); err != nil {
		fmt.Printf("Failed to select 1: %s", err)
		return
	}

	var id int
	if err = row.Scan(&id); err != nil {
		fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
		return
	}

	fmt.Printf("Test1 id: %d \n", id)

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		time.Sleep(time.Millisecond * 500)

		fmt.Println("Lock 1 FOR UPDATE")
		if _, err = tx1.Exec(`UPDATE invoices SET name = 'test_11' WHERE id = 1`); err != nil {
			fmt.Printf("failed exec update invoice: %s", err)
			return err
		}

		if err = tx1.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}

		fmt.Println("Transaction 1 committed")

		return nil
	})

	group.Go(func() error {
		fmt.Println("Lock 2 Select FOR UPDATE")
		row = tx2.QueryRow(`SELECT id FROM invoices Where id = 1 FOR UPDATE`)

		if err = row.Err(); err != nil {
			fmt.Printf("Failed to select 2: %s", err)
			return err
		}

		if err = row.Scan(&id); err != nil {
			fmt.Printf("failed to scan 2 invoiceAmount: %s", err)
			return err
		}

		fmt.Printf("Test2 id: %d \n", id)

		fmt.Println("Lock 2 FOR UPDATE")
		if _, err = tx2.Exec(`UPDATE invoices SET name = 'test_1' WHERE id = 1`); err != nil {
			fmt.Printf("failed exec update invoice: %s", err)
			return err
		}

		if err = tx2.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}
		fmt.Println("Transaction 2 committed")

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	return
}

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

func TestLostUpdateBetweenTransactionAndBasic(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Lost Update between transaction and basic-----------------")

	//create transaction
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction created")

	//select and print current invoice sum
	var (
		row           *sql.Row
		invoiceAmount int
	)
	if row = db.QueryRow(`Select amount from invoices WHERE id = 1`); err != nil {
		fmt.Printf("failed select invoices: %s", err)
		return
	}

	if err = row.Scan(&invoiceAmount); err != nil {
		fmt.Printf("failed to scan 1 invoiceAmount: %s", err)
		return
	}

	fmt.Printf("Invoice sum before update inside and outside of transaction: %d \n", invoiceAmount)

	//Update invoice inside of transaction
	if _, err = tx.Exec(`Update invoices SET amount = (amount + 200) WHERE id = 1`); err != nil {
		fmt.Printf("failed exec update invoice: %s", err)
		return
	}

	//Update invoice outside of transaction - share lock in postgres read committed/snapshot_isolation
	//(where is mvcc ??) it is a pessimistic lock
	if _, err = db.Exec(`Update invoices SET amount = (amount + 500) where id = 1`); err != nil {
		fmt.Printf("failed exec update invoices: %s", err)
		return
	}

	if err = tx.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	fmt.Println("Transaction committed")

	//select invoice again
	row = db.QueryRow(`Select amount from invoices Where id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select 2 invoiceAmount: %s", err)
		return
	}

	if err = row.Scan(&invoiceAmount); err != nil {
		fmt.Printf("failed to scan 2 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice amount after two updates(should be 1700): %d \n", invoiceAmount)

	return
}

func TestLostUpdateBetweenTransactionAndTransactionReadAndUpdate(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Lost Update between Transaction And Transaction Read and Update-----------------")

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

	var (
		row        *sql.Row
		invoiceSum int
	)
	if row = db.QueryRow(`Select amount from invoices where id = 1`); err != nil {
		fmt.Printf("failed select invoice sum: %s", err)
		return err
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 1 invoiceSum: %s", err)
		return err
	}

	fmt.Printf("Invoice sum before update in transactions: %d \n", invoiceSum)

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {

		//select and print current invoice sum
		var invoiceSum1 int
		row1 := tx1.QueryRow(`Select amount from invoices where id = 1`)

		if err = row1.Scan(&invoiceSum1); err != nil {
			fmt.Printf("failed to scan 1 invoiceSum: %s", err)
			return err
		}

		fmt.Printf("invoiceSum1 sum before update in transactions: %d \n", invoiceSum1)

		time.Sleep(time.Millisecond * 500)

		//update invoice in 1 transaction
		if _, err = tx1.Exec(`Update invoices set amount = $1 + 500 WHERE id = 1`, invoiceSum1); err != nil {
			fmt.Printf("failed exec update invoice tx1: %s", err)
			return err
		}

		if err = tx1.Commit(); err != nil {
			fmt.Printf("error to commit transaction 1: %s", err)
			return err
		}
		fmt.Println("Transaction 1 committed")

		return nil
	})

	group.Go(func() error {
		var invoiceSum2 int
		row2 := db.QueryRow(`Select amount from invoices where id = 1`)

		if err = row2.Scan(&invoiceSum2); err != nil {
			fmt.Printf("failed to scan 2 invoiceSum: %s", err)
			return err
		}

		fmt.Printf("invoiceSum2 sum before update in transactions: %d \n", invoiceSum2)

		time.Sleep(time.Millisecond * 500)

		//update invoice in 2 transaction
		if _, err = tx2.Exec(`Update invoices set amount = $1 + 200 WHERE id = 1`, invoiceSum2); err != nil {
			fmt.Printf("failed exec update invoice tx2: %s", err)
			return err
		}

		if err = tx2.Commit(); err != nil {
			fmt.Printf("error to commit 2 transaction: %s", err)
			return err
		}

		fmt.Println("Transaction 2 created")

		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	//select invoice again
	row = db.QueryRow(`Select amount from invoices WHERE id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select amount: %s", err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan invoice: %s", err)
		return
	}

	fmt.Printf("Invoice count after update both transaction(must be 1700): %d \n", invoiceSum)

	return
}

func TestLostUpdateBetweenTransactionAndTransactionAtomicUpdate(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Lost Update between Transaction And Transaction Atomic Update-----------------")

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

	//select and print current invoice sum
	var (
		row        *sql.Row
		invoiceSum int
	)
	if row = db.QueryRow(`Select amount from invoices where id = 1`); err != nil {
		fmt.Printf("failed select invoice sum: %s", err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 1 invoiceSum: %s", err)
		return
	}

	fmt.Printf("Invoice sum before update in transactions: %d \n", invoiceSum)

	//update invoice in 1 transaction
	if _, err = tx1.Exec(`Update invoices set amount = amount + 500 WHERE id = 1`); err != nil {
		fmt.Printf("failed exec update invoice tx1: %s", err)
		return err
	}

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {

		time.Sleep(time.Millisecond * 400)

		if err = tx1.Commit(); err != nil {
			fmt.Printf("error to commit transaction 1: %s", err)
			return err
		}

		return nil
	})

	group.Go(func() error {
		//update invoice in 1 transaction
		if _, err = tx2.Exec(`Update invoices set amount = amount + 200 WHERE id = 1`); err != nil {
			fmt.Printf("failed exec update invoice tx2: %s", err)
			return err
		}
		return nil
	})

	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	fmt.Println("Transaction 1 committed")

	if err = tx2.Commit(); err != nil {
		fmt.Printf("error to commit 2 transaction: %s", err)
		return
	}

	fmt.Println("Transaction 2 committed")

	//select invoice again
	row = db.QueryRow(`Select amount from invoices WHERE id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select amount: %s", err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 2 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice count after update both transaction(must be 1700): %d \n", invoiceSum)

	return
}

func TestPhantomReadBetweenTransactionAndBasic(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Phantom Read between Transaction And Basic-----------------")

	//create transaction
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction created")

	//select and print current invoice sum
	var (
		row       *sql.Row
		countRows int
	)
	if row = tx.QueryRow(`Select count(*) from invoices`); err != nil {
		fmt.Printf("failed select invoices: %s", err)
		return
	}

	if err = row.Scan(&countRows); err != nil {
		fmt.Printf("failed to scan 1 countRows: %s", err)
		return
	}

	fmt.Printf("Invoice count before insert outside of transaction: %d \n", countRows)

	//insert invoice outside of transaction
	if _, err = db.Exec(`INSERT  into invoices(id, name, amount) VALUES (2, 'test_2', 2000)`); err != nil {
		fmt.Printf("failed exec create new invoice: %s", err)
		return
	}

	//select invoice again
	row = tx.QueryRow(`Select count(*) from invoices`)

	if row.Err() != nil {
		fmt.Printf("failed select 2 countRows: %s", err)
		return
	}

	if err = row.Scan(&countRows); err != nil {
		fmt.Printf("failed to scan 2 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice count after insert outside of transaction: %d \n", countRows)

	if err = tx.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	fmt.Println("Transaction committed")

	return
}

func TestPhantomReadBetweenTransactionAndTransaction(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Phantom Read between Transaction And Transaction-----------------")

	//create transaction 1
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
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

	//select and print current invoice sum
	var (
		countInvoices int64
		row           *sql.Row
	)
	row = tx2.QueryRow(`Select count(*) from invoices`)

	if row.Err() != nil {
		fmt.Printf("failed select 1 invoice: %s", row.Err())
		return
	}

	if err = row.Scan(&countInvoices); err != nil {
		fmt.Printf("failed to scan 1 invoice: %s", err)
		return
	}

	fmt.Printf("CountInvoices in transaction 2 before insert in transaction 1: %d \n", countInvoices)

	//change invoices num in transaction 1
	if _, err = tx.Exec(`INSERT  into invoices(id, name, amount) VALUES (2, 'test_2', 2000)`); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	if err = tx.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	fmt.Println("Transaction 1 committed")

	//select invoice again
	row = tx2.QueryRow(`Select count(*) from invoices`)

	if row.Err() != nil {
		fmt.Printf("failed select 2 invoice: %s", err)
		return
	}

	if err = row.Scan(&countInvoices); err != nil {
		fmt.Printf("failed to scan 2 invoice: %s", err)
		return
	}

	fmt.Printf("Count Invoices in transaction 2 after insert in transaction 1: %d \n", countInvoices)

	if err = tx2.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	fmt.Println("Transaction 2 committed")

	return nil
}

func TestShareBlockAndUpdateOutsideTransaction(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Share Lock-----------------")

	//create transaction
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction created")

	//select and print current invoice sum
	var (
		invoiceSum int64
		row        *sql.Row
	)
	row = tx.QueryRow(`Select amount from invoices WHERE id = 1 for share`)

	if row.Err() != nil {
		fmt.Printf("failed select 1 invoice: %s", row.Err())
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 1 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum after share lock inside of transaction: %d \n", invoiceSum)

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		time.Sleep(time.Millisecond * 500)

		//select invoice again
		row = tx.QueryRow(`Select amount from invoices WHERE id = 1`)

		if row.Err() != nil {
			fmt.Printf("failed select 2 invoice: %s", err)
			return err
		}

		if err = row.Scan(&invoiceSum); err != nil {
			fmt.Printf("failed to scan 2 invoice: %s", err)
			return err
		}

		fmt.Printf("Invoice sum inside transaction before update outside of transaction: %d \n", invoiceSum)

		if err = tx.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}

		fmt.Println("Transaction committed")
		return nil
	})
	group.Go(func() error {

		row = db.QueryRow(`Select amount from invoices WHERE id = 1`)

		if row.Err() != nil {
			fmt.Printf("failed select 2 invoice: %s", err)
			return err
		}

		if err = row.Scan(&invoiceSum); err != nil {
			fmt.Printf("failed to scan 2 invoice: %s", err)
			return err
		}

		fmt.Printf("Invoice sum outside transaction before update outside of transaction: %d \n", invoiceSum)

		//change invoice outside of transaction
		if _, err = db.Exec(`UPDATE invoices SET amount = 1500 Where id = $1`, 1); err != nil {
			fmt.Printf("failed exec create invoices: %s", err)
			return err
		}

		row = db.QueryRow(`Select amount from invoices WHERE id = 1`)

		if row.Err() != nil {
			fmt.Printf("failed select 2 invoice: %s", err)
			return err
		}

		if err = row.Scan(&invoiceSum); err != nil {
			fmt.Printf("failed to scan 2 invoice: %s", err)
			return err
		}

		fmt.Printf("Invoice sum after commit of transaction: %d \n", invoiceSum)

		return nil
	})
	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	return nil
}

func TestExclusiveBlockAndReadOutsideTransaction(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Exclusive lock-----------------")

	//create transaction
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction created")

	//select and print current invoice sum
	var (
		invoiceSum int64
		row        *sql.Row
	)
	row = tx.QueryRow(`Select amount from invoices WHERE id = 1 for update`)

	if row.Err() != nil {
		fmt.Printf("failed select 1 invoice: %s", row.Err())
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 1 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum before update outside of transaction: %d \n", invoiceSum)

	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		time.Sleep(time.Millisecond * 500)
		//change invoice inside of transaction
		if _, err = tx.Exec(`UPDATE invoices SET amount = 1500 Where id = $1`, 1); err != nil {
			fmt.Printf("failed exec create invoices: %s", err)
			return err
		}

		if err = tx.Commit(); err != nil {
			fmt.Printf("error to commit transaction: %s", err)
			return err
		}

		fmt.Println("Transaction committed")
		return nil
	})
	group.Go(func() error {

		//select invoice again
		row = db.QueryRow(`Select amount from invoices WHERE id = 1`)

		if row.Err() != nil {
			fmt.Printf("failed select 2 invoice: %s", err)
			return err
		}

		if err = row.Scan(&invoiceSum); err != nil {
			fmt.Printf("failed to scan 2 invoice: %s", err)
			return err
		}

		fmt.Printf("Invoice sum after update outside of transaction: %d \n", invoiceSum)
		return nil
	})
	err = group.Wait()
	if err != nil {
		fmt.Printf("waitgroup error: %s", err)
		return
	}

	return nil
}

func TestUncommittedDirtyReadByAnotherTransaction(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Dirty read by another transaction-----------------")

	//create transaction 1
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
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

	//select and print current invoice sum
	var (
		invoiceSum int64
		row        *sql.Row
	)
	row = tx2.QueryRow(`Select amount from invoices WHERE id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select 1 invoice: %s", row.Err())
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 1 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum in transaction 2 before update in transaction 1: %d \n", invoiceSum)

	//change invoice in transaction 1
	if _, err = tx.Exec(`UPDATE invoices SET amount = 1500 Where id = $1`, 1); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	//select invoice again
	row = tx2.QueryRow(`Select amount from invoices WHERE id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select 2 invoice: %s", err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 2 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum in transaction 2 after update in transaction 1: %d \n", invoiceSum)

	if err = tx.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	fmt.Println("Transaction 1 committed")

	if err = tx2.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	fmt.Println("Transaction 2 committed")

	return nil
}

// TestUncommittedDirtyReadByBasicQuery update not committed transaction - outside transaction
// basic transaction can see this update
func TestUncommittedDirtyReadByBasicQuery(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Dirty read by basic query outside of transaction----------------")

	//create transaction
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction created")

	//select and print current invoice sum
	var (
		invoiceSum int64
		row        *sql.Row
	)
	row = db.QueryRow(`Select amount from invoices WHERE id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select 1 invoice: %s", row.Err())
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 1 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum before update in transaction: %d \n", invoiceSum)

	//change invoice outside of transaction
	if _, err = tx.Exec(`UPDATE invoices SET amount = 1500 Where id = $1`, 1); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	//select invoice again
	row = db.QueryRow(`Select amount from invoices WHERE id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select 2 invoice: %s", err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 2 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum after update in transaction: %d \n", invoiceSum)

	if err = tx.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	fmt.Println("Transaction committed")

	return nil
}

// TestUncommittedNotRepeatableRead - start transaction, read invoice,
// than update invoice outside of transaction - transaction can see changes.
func TestUncommittedNotRepeatableRead(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Not repeatable read-----------------")

	//create transaction
	var tx *sqlx.Tx
	if tx, err = db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: txLevel,
		ReadOnly:  false,
	}); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	fmt.Println("Transaction created")

	//select and print current invoice sum
	var (
		invoiceSum int64
		row        *sql.Row
	)
	row = tx.QueryRow(`Select amount from invoices WHERE id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select 1 invoice: %s", row.Err())
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 1 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum before update outside of transaction: %d \n", invoiceSum)

	//change invoice outside of transaction
	if _, err = db.Exec(`UPDATE invoices SET amount = 1500 Where id = $1`, 1); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	//select invoice again
	row = tx.QueryRow(`Select amount from invoices WHERE id = 1`)

	if row.Err() != nil {
		fmt.Printf("failed select 2 invoice: %s", err)
		return
	}

	if err = row.Scan(&invoiceSum); err != nil {
		fmt.Printf("failed to scan 2 invoice: %s", err)
		return
	}

	fmt.Printf("Invoice sum after update outside of transaction: %d \n", invoiceSum)

	if err = tx.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}

	fmt.Println("Transaction committed")

	return nil
}
