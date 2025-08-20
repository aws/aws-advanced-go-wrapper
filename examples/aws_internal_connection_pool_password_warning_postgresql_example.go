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

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/internal_pool"
	_ "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
)

func main() {
	// Database connection string
	host := "endpoint"
	port := "5432"
	user := "user"
	password := "password"
	dbName := "db"
	plugins := "failover,efm,readWriteSplitting"

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
		host, port, user, password, dbName, plugins,
	)

	badPassConnStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
		host, port, user, "BadPassword", dbName, plugins,
	)

	// Setup internal pool
	poolOptions := internal_pool.NewInternalPoolOptions(
		internal_pool.WithMaxIdleConns(2),                                // default value is 2
		internal_pool.WithMaxConnLifetime(time.Duration(10)*time.Minute), // default is 0, infinite life time
		internal_pool.WithMaxConnIdleTime(time.Duration(10)*time.Minute), // default is 0, infinite idle time
	)

	provider := internal_pool.NewInternalPooledConnectionProvider(
		poolOptions,
		0,
	)

	driver_infrastructure.SetCustomConnectionProvider(provider)

	// db1 will use the correct password and it will work
	db1, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	testQuery(db1)
	db1.Close()

	// db2 will use the incorrect password, but it will still be able to connect
	db2, err := sql.Open("awssql-pgx", badPassConnStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	testQuery(db2)
	db2.Close()

	// Remove all pools
	provider.ReleaseResources()

	// db3 will fail to connect due to incorrect password
	db3, err := sql.Open("awssql-pgx", badPassConnStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	testQuery(db3)
	db3.Close()
}

func testQuery(db *sql.DB) {
	var result int
	err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}
	fmt.Println("Query result:", result)
}
