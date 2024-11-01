package helper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
)

const (
	AccountIdStr = "1"
	AccountIdInt = 1
)

type Transaction struct {
	tx             *sqlx.Tx
	transactionNum int
	isCommitted    bool
	isRollbacked   bool
	dbName         string
}

const DB = "transaction_isolation"

func (t *Transaction) PrintAccountsSumByUserID(userID int) (err error) {
	row := t.tx.QueryRow(fmt.Sprintf(`Select sum(amount) from accounts WHERE user_id = '%d'`, userID))

	if err = row.Err(); err != nil {
		fmt.Printf("failed select account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	var accountsSum int64
	if err = row.Scan(&accountsSum); err != nil {
		fmt.Printf("failed to scan account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	fmt.Printf("Account sum in transaction %d: %d \n", t.transactionNum, accountsSum)
	return
}

func (t *Transaction) PrintAmount() (err error) {
	row := t.tx.QueryRow(`Select amount from accounts WHERE id = ` + AccountIdStr)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	var accountSum int64
	if err = row.Scan(&accountSum); err != nil {
		fmt.Printf("failed to scan account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	fmt.Printf("Account sum in transaction %d: %d \n", t.transactionNum, accountSum)
	return
}

func (t *Transaction) GetAmountWithShareLock() (accountSum int64, err error) {
	query := fmt.Sprintf("Select amount from accounts WHERE id = %d FOR SHARE", AccountIdInt)
	if t.dbName == "sqlserver" {
		query = fmt.Sprintf("Select amount from accounts WITH (HOLDLOCK, ROWLOCK) WHERE id = %d", AccountIdInt)
	}

	row := t.tx.QueryRow(query)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	if err = row.Scan(&accountSum); err != nil {
		fmt.Printf("failed to scan account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	return
}

func (t *Transaction) GetAmountWithExclusiveLock() (accountSum int64, err error) {
	query := fmt.Sprintf("Select amount from accounts WHERE id = %d FOR UPDATE", AccountIdInt)
	if t.dbName == "sqlserver" {
		query = fmt.Sprintf("Select amount from accounts WITH (UPDLOCK) WHERE id = %d", AccountIdInt)
	}

	row := t.tx.QueryRow(query)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	if err = row.Scan(&accountSum); err != nil {
		fmt.Printf("failed to scan account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	return
}

func (t *Transaction) GetAmount() (accountSum int64, err error) {
	row := t.tx.QueryRow(`Select amount from accounts WHERE id = ` + AccountIdStr)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	if err = row.Scan(&accountSum); err != nil {
		fmt.Printf("failed to scan account by transaction %d: %s\n", t.transactionNum, err)
		return
	}

	return
}

func (t *Transaction) Withdrawal2(accountId int, dbName string) (err error) {
	query := fmt.Sprintf(`
			UPDATE accounts AS t1
    		JOIN (
        		SELECT SUM(amount) AS total_amount FROM accounts WHERE user_id = 1
    		) AS t2
			SET t1.amount = t1.amount - 1000
			WHERE t1.id = %d AND t1.amount >= 1000 AND t2.total_amount >= 2000;
		`, accountId)
	if _, err = t.tx.Exec(query); err != nil {
		fmt.Printf("error withdrawal2 %d: %s \n", t.transactionNum, err)
	}
	return
}

func (t *Transaction) Withdrawal(accountId int, dbName string) (err error) {
	if dbName == "mysql" {
		querySetTotalAmount := `
			SELECT SUM(amount) INTO @total_amount FROM transaction_isolation.accounts WHERE user_id = 1;
		`
		if _, err = t.tx.Exec(querySetTotalAmount); err != nil {
			fmt.Printf("failed set @total_amount in transaction %d: %s \n", t.transactionNum, err)
		}
		queryUpdate := fmt.Sprintf(`
			UPDATE accounts
			SET amount = amount - 1000
			WHERE id = %d AND amount >= 1000 AND @total_amount >= 2000
		`, accountId)
		if _, err = t.tx.Exec(queryUpdate); err != nil {
			fmt.Printf("failed update amount in transaction %d: %s \n", t.transactionNum, err)
		}

		return
	}

	query := fmt.Sprintf(`UPDATE accounts SET amount = amount-1000 WHERE id = %d AND amount >=1000 AND  (
      			SELECT SUM(amount) FROM accounts WHERE user_id = 1
		) >= 2000`, accountId)

	if _, err = t.tx.Exec(query); err != nil {
		fmt.Printf("failed exec withdrawal in transaction %d: %s \n", t.transactionNum, err)
	}
	return
}

func (t *Transaction) UpdateAccountId(newAmount int64, isIncrement bool) (err error) {
	fmt.Printf("Update account amount in transaction %d to %d \n", t.transactionNum, newAmount)

	queryLeftSide := `UPDATE accounts SET amount = `
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
		query = queryLeftSide + ` @Amount WHERE id = @AccountID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("AccountID", 2)); err != nil {
			fmt.Printf("failed exec update account in transaction %d: %s \n", t.transactionNum, err)
		}
		return
	case "oracle":
		query = queryLeftSide + ` :Amount WHERE id = :AccountID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("AccountID", 2)); err != nil {
			fmt.Printf("failed exec update account in transaction %d: %s \n", t.transactionNum, err)
		}
		return
	default:
		query = `UPDATE accounts SET amount = $1 Where id = $2`
	}

	if _, err = t.tx.Exec(query, newAmount, 2); err != nil {
		fmt.Printf("failed exec update account in transaction %d: %s \n", t.transactionNum, err)
		return
	}
	return
}

func (t *Transaction) UpdateAccount(newAmount int64) (err error) {
	fmt.Printf("Update account amount in transaction %d to %d \n", t.transactionNum, newAmount)

	queryLeftSide := `UPDATE accounts SET amount = `

	var query string
	switch t.dbName {
	case "postgres":
		query = queryLeftSide + ` $1 WHERE id = $2`
	case "mysql":
		query = queryLeftSide + ` ? WHERE id = ?`
	case "sqlserver":
		query = queryLeftSide + ` @Amount WHERE id = @AccountID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("AccountID", AccountIdInt)); err != nil {
			fmt.Printf("failed exec update account in transaction %d: %s \n", t.transactionNum, err)
		}
		return
	case "oracle":
		query = queryLeftSide + ` :Amount WHERE id = :AccountID`
		if _, err = t.tx.Exec(query, sql.Named("Amount", newAmount), sql.Named("AccountID", AccountIdInt)); err != nil {
			fmt.Printf("failed exec update account in transaction %d: %s \n", t.transactionNum, err)
		}
		return
	default:
		query = `UPDATE accounts SET amount = $1 Where id = $2`
	}

	if _, err = t.tx.Exec(query, newAmount, AccountIdInt); err != nil {
		fmt.Printf("failed exec update account in transaction %d: %s \n", t.transactionNum, err)
		return
	}
	return
}

func (t *Transaction) DeleteAccount(accountId int) (err error) {
	if _, err = t.tx.Exec(fmt.Sprintf(
		fmt.Sprintf("DELETE FROM accounts WHERE id = %d", accountId),
	)); err != nil {
		fmt.Printf("failed exec delete account: %s", err)
		return
	}
	return nil
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

func (t *Transaction) GetTx() *sqlx.Tx {
	return t.tx
}
