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
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/awsctx"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/internal_pool"
	_ "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
)

func main() {
	// Database connection string
	host := "endpoint"
	port := "3306"
	user := "user"
	password := "password"
	dbName := "db"
	plugins := "readWriteSplitting,efm,failover"

	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?plugins=%s",
		user, password, host, port, dbName, plugins,
	)

	// Optional: Setup internal pool (recommended if executing queries through sql.DB object)
	poolOptions := internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(2),                                // default value is 2
		internal_pool.WithMaxConnLifetime(time.Duration(10)*time.Minute), // default is 0, infinite life time
		internal_pool.WithMaxConnIdleTime(time.Duration(10)*time.Minute), // default is 0, infinite idle time
	)

	provider := internal_pool.NewInternalPooledConnectionProvider(
		poolOptions,
		0, // if 0 is given, it will be set to a default of 30 minutes
	)

	// Or if you wish to use a custom pool key function:

	// provider := internal_pool.NewInternalPooledConnectionProviderWithPoolKeyFunc(
	// 	poolOptions,
	// 	0,
	// 	func(host *host_info_util.HostInfo, props map[string]string) string {
	// 		url := host.GetUrl()
	// 		user := props["user"]
	// 		return url + user
	// 	},
	// )

	// Setup Aws Driver ot use internal pooled connection provider
	driver_infrastructure.SetCustomConnectionProvider(provider)

	// Open the database connection
	db, err := sql.Open("awssql-mysql", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Setup Step: Open connection and create tables - uncomment this section to create table and test values.
	// _, err = execWithFailoverHandling(db, "CREATE TABLE bank_test (name varchar(40), account_balance int)")
	// if err != nil {
	// 	// Additional error handling can be added here. See transaction step for an example.
	// 	log.Fatal("There was an issue creating bank_test:", err)
	// }
	// _, err = execWithFailoverHandling(db, "INSERT INTO bank_test VALUES ('Jane Doe', 200), ('John Smith', 200)")
	// if err != nil {
	// 	// Additional error handling can be added here. See transaction step for an example.
	// 	log.Fatal("There was an issue inserting bank_test:", err)
	// }

	// Perform transaction.
	err = queryWithFailoverHandling(db, "SELECT * FROM bank_test WHERE name='John Smith'")
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

	// To be called at the end of a program to clear any remaining cache items:
	driver.ClearCaches()

	//To be called if using internal connection pool provider above
	// provider.ReleaseResources()
}

func queryWithFailoverHandling(db *sql.DB, query string) error {
	// setup readOnly context
	ctx := context.WithValue(context.Background(), awsctx.SetReadOnly, true)
	rows, err := db.QueryContext(ctx, query)

	if err != nil {
		if error_util.IsType(err, error_util.FailoverFailedErrorType) {
			// Connection failed and the wrapper failed to reconnect to a new instance.
			return err
		} else if error_util.IsType(err, error_util.FailoverSuccessErrorType) {
			// Query execution failed and the wrapper successfully failed over to a new elected writer instance.
			// Re-run query.
			rows, err = db.QueryContext(context.TODO(), query)
			if err != nil {
				return err
			}
		} else if error_util.IsType(err, error_util.TransactionResolutionUnknownErrorType) {
			// Transaction resolution unknown. Please re-configure session state if required and try
			// restarting transaction.
			return err
		} else {
			return err
		}
	}
	defer rows.Close()

	for rows.Next() {
		var col1, col2 string
		err := rows.Scan(&col1, &col2)
		if err != nil {
			return err
		}
		fmt.Printf("Column 1: %s, Column 2: %s\n", col1, col2)
	}
	return rows.Err()
}

func execWithFailoverHandling(db *sql.DB, query string) (sql.Result, error) {
	exec, err := db.ExecContext(context.TODO(), query)

	if err != nil {
		if error_util.IsType(err, error_util.FailoverFailedErrorType) {
			// Connection failed and the wrapper failed to reconnect to a new instance.
			return nil, err
		} else if error_util.IsType(err, error_util.FailoverSuccessErrorType) {
			// Query execution failed and the wrapper successfully failed over to a new elected writer instance.
			// Re-run query.
			return db.ExecContext(context.TODO(), query)
		} else if error_util.IsType(err, error_util.TransactionResolutionUnknownErrorType) {
			// Transaction resolution unknown. Please re-configure session state if required and try
			// restarting transaction.
			return nil, err
		}
	}
	return exec, err
}
