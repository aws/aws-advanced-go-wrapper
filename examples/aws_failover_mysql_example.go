/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package examples

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	_ "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
)

func main() {
	// Database connection string
	host := "endpoint"
	port := "3306"
	user := "user"
	password := "password"
	dbName := "db"

	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		user, password, host, port, dbName,
	)

	// Open the database connection
	db, err := sql.Open("awssql-mysql", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	err = setInitialSessionSettings(db)
	if err != nil {
		// Additional error handling can be added here. See transaction step for an example.
		log.Fatal("There was an issue setting initial session settings:", err)
	}

	// Setup Step: Open connection and create tables - uncomment this section to create table and test values.
	//_, err = queryWithFailoverHandling(db, "CREATE TABLE bank_test (name varchar(40), account_balance int)")
	//if err != nil {
	//	// Additional error handling can be added here. See transaction step for an example.
	//	log.Fatal("There was an issue creating bank_test:", err)
	//}
	//_, err = queryWithFailoverHandling(db, "INSERT INTO bank_test VALUES ('Jane Doe', 200), ('John Smith', 200)")
	//if err != nil {
	//	// Additional error handling can be added here. See transaction step for an example.
	//	log.Fatal("There was an issue inserting bank_test:", err)
	//}

	// Perform transaction.
	result, err := queryWithFailoverHandling(db, "UPDATE bank_test SET account_balance=account_balance - 100 WHERE name='Jane Doe'")
	if err != nil {
		if error_util.IsType(err, error_util.FailoverFailedErrorType) {
			// User application should open a new connection, check the results of the failed transaction and re-run it if
			// needed. See:
			// https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheFailoverPlugin.md#failoverfailederror
			log.Fatal("There was an issue updating bank_test:", err)
		} else if error_util.IsType(err, error_util.TransactionResolutionUnknownErrorType) {
			// User application should check the status of the failed transaction and restart it if needed. See:
			// https://github.com/aws/aws-advanced-go-wrapper/blob/main/docs/user-guide/using-plugins/UsingTheFailoverPlugin.md#transactionresolutionunknownerror
			log.Fatal("There was an issue updating bank_test:", err)
		} else {
			// Unexpected error unrelated to failover. This should be handled by the user application.
			log.Fatal("There was an issue updating bank_test:", err)
		}
	}

	fmt.Println(result.RowsAffected())

	// To be called at the end of a program to clear any remaining cache items:
	driver.ClearCaches()
}

func setInitialSessionSettings(db *sql.DB) error {
	// User can edit settings.
	_, err := db.Exec("SET time_zone = 'UTC'")
	if err != nil {
		return err
	}
	return nil
}

func queryWithFailoverHandling(db *sql.DB, query string) (sql.Result, error) {
	exec, err := db.Exec(query)
	if err != nil {
		if error_util.IsType(err, error_util.FailoverFailedErrorType) {
			// Connection failed and the wrapper failed to reconnect to a new instance.
			return nil, err
		} else if error_util.IsType(err, error_util.FailoverSuccessErrorType) {
			// Query execution failed and the wrapper successfully failed over to a new elected writer instance.
			// Reconfigure the connection.
			err = setInitialSessionSettings(db)
			if err != nil {
				return nil, err
			}
			// Re-run query.
			return db.Exec(query)
		} else if error_util.IsType(err, error_util.TransactionResolutionUnknownErrorType) {
			// Transaction resolution unknown. Please re-configure session state if required and try
			// restarting transaction.
			return nil, err
		}
	}
	return exec, err
}
