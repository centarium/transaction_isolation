package helper

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

func PrintAmount(db *sqlx.DB) (err error) {
	row := db.QueryRow(`Select amount from accounts WHERE id = ` + AccountIdStr)

	if err = row.Err(); err != nil {
		fmt.Printf("failed select account: %s\n", err)
		return
	}

	var accountSum int64
	if err = row.Scan(&accountSum); err != nil {
		fmt.Printf("failed to scan account: %s\n", err)
		return
	}

	fmt.Printf("Account sum: %d \n", accountSum)
	return
}

func DropAndCreateAccount(db *sqlx.DB, dbName string) (err error) {
	if err = TruncateAccounts(db, dbName); err != nil {
		return err
	}

	//create account for tests
	if err = CreateAccount(db, AccountIdInt); err != nil {
		return err
	}

	return nil
}

func TruncateAccounts(db *sqlx.DB, dbName string) (err error) {
	truncateString := `TRUNCATE accounts`
	switch dbName {
	case "sqlserver":
		truncateString = `TRUNCATE table accounts`
	case "oracle":
		truncateString = `TRUNCATE table SYSTEM.accounts`
	}

	if _, err = db.Exec(truncateString); err != nil {
		fmt.Printf("failed exec drop test account: %s", err)
		return
	}
	return nil
}

func CreateAccount(db *sqlx.DB, accountId int) (err error) {
	if _, err = db.Exec(fmt.Sprintf(
		"INSERT  into accounts(id, user_id, amount) VALUES (%d, 1, 1000)", accountId,
	)); err != nil {
		fmt.Printf("failed exec create accounts: %s", err)
		return
	}
	return nil
}

func DeleteAccount(db *sqlx.DB, accountId int) (err error) {
	if _, err = db.Exec(fmt.Sprintf(
		fmt.Sprintf("DELETE FROM accounts WHERE id = %d", accountId),
	)); err != nil {
		fmt.Printf("failed exec delete account: %s", err)
		return
	}
	return nil
}

func PrintUserAccountsSum(db *sqlx.DB, userId int) (err error) {
	row := db.QueryRow(fmt.Sprintf(`Select SUM(amount) from accounts WHERE user_id = %d`, userId))

	if err = row.Err(); err != nil {
		fmt.Printf("failed select account: %s\n", err)
		return
	}

	var accountSum int64
	if err = row.Scan(&accountSum); err != nil {
		fmt.Printf("failed to scan account: %s\n", err)
		return
	}

	fmt.Printf("User accounts sum: %d \n", accountSum)
	return
}
