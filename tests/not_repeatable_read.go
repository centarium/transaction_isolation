package tests

// TestUncommittedNotRepeatableRead - start transaction, read invoice,
// then update invoice outside of transaction - transaction can see changes.
/*func TestUncommittedNotRepeatableRead(ctx context.Context, db *sqlx.DB, txLevel sql.IsolationLevel) (err error) {
	fmt.Println("----------------Not repeatable read-----------------")

	var tx *sqlx.Tx
	if tx, err = helper.CreateTransaction(ctx, db, txLevel, 1); err != nil {
		fmt.Printf("failed to create transaction: %s", err)
		return
	}

	//select and print current invoice sum
	var invoiceSum int64
	if invoiceSum, err = helper.GetAmount(tx, 1); err != nil {
		fmt.Printf("failed to GetAmount before update: %s", err)
		return
	}
	fmt.Printf("Update outside of transaction \n")

	//change invoice outside of transaction
	if _, err = db.Exec(`UPDATE invoices SET amount = 1500 Where id = $1`, 1); err != nil {
		fmt.Printf("failed exec create invoices: %s", err)
		return
	}

	if invoiceSum, err = helper.GetAmount(tx, 1); err != nil {
		fmt.Printf("failed to GetAmount after update: %s", err)
	}
	fmt.Printf("Invoice sum after update outside of transaction: %d \n", invoiceSum)

	if err = tx.Commit(); err != nil {
		fmt.Printf("error to commit transaction: %s", err)
		return
	}
	fmt.Println("Transaction committed")
	return nil
}*/
