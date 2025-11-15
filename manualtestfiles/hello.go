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

package main

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	//_ "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
	_ "github.com/aws/aws-advanced-go-wrapper/iam"
	_ "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	_ "github.com/aws/aws-advanced-go-wrapper/okta"
	_ "github.com/aws/aws-advanced-go-wrapper/pgx-driver" // awssql pgx driver
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	if os.Getenv("APP_ENV") == "development" {
		log.Println("Enabling pprof for profiling")
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	for true {
		bg()
		time.Sleep(30 * time.Second)
	}
}

func bg() {
	host := "someDbHost"
	port := "5432"
	user := "someUser"
	password := "somePassword"
	dbName := "postgres"
	plugins := "failover,efm,bg"

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
		host, port, user, password, dbName, plugins,
	)

	// alternative connection string for pgx
	// connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?plugins=%s&dbUser=%s&iamRegion=%s&idpEndpoint=%s&appId=%s&iamRoleArn=%s&iamIdpArn=%s&idpUsername=%s&idpPassword=%s",
	// 	user, password, host, port, dbName, plugins, dbUser, iamRegion, idpEndpoint, appId, iamRoleArn, iamIdpArn, idpUsername, idpPassword)

	// Open the database connection
	db, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Execute the query

	var result int
	err = db.QueryRow("SELECT 2").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}

	// Print the result
	fmt.Println("Query result:", result)

}

func custom() {
	host := "someDbHost"
	port := "5432"
	user := "someUser"
	password := "somePassword"
	dbName := "postgres"
	plugins := "customEndpoint"

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s customEndpointRegion=us-east-2",
		host, port, user, password, dbName, plugins,
	)

	db, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Execute the query
	var result string
	err = db.QueryRow("SELECT aurora_db_instance_identifier()").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}

	// Print the result
	fmt.Println("Query result:", result)
}

func multiazmysql() {
	host := "someDbHost"
	port := "3306"
	user := "someUser"
	dbName := "test_database"
	plugins := "iam"
	tlsConfig := "skip-verify"
	iamRegion := "eu-west-1"

	connStr := fmt.Sprintf("%s:@tcp(%s:%s)/%s?plugins=%s&allowCleartextPasswords=true&tls=%s&iamRegion=%s",
		user, host, port, dbName, plugins, tlsConfig, iamRegion,
	)

	db, err := sql.Open("awssql-mysql", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Execute the query
	var result string
	//err = db.Ping()
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}

	// Print the result
	fmt.Println("Query result:", result)
}

func multiazpgping() {
	host := "someDbHost"
	port := "5432"
	user := "someUser"
	password := "anypassword"
	dbName := "test_database"
	plugins := "failover,iam"

	//connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
	//	host, port, user, password, dbName, plugins,
	//)
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s iamRegion=eu-west-1",
		host, port, user, password, dbName, plugins,
	)

	db, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Execute the query
	var result string
	//err = db.Ping()
	err = db.QueryRow("SELECT 1 AS tmp FROM information_schema.routines WHERE routine_schema='rds_tools' AND routine_name='multi_az_db_cluster_source_dbi_resource_id'").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}

	// Print the result
	fmt.Println("Query result:", result)
}

func limitless() {
	host := "someDbHost"
	port := "5432"
	user := "someUser"
	password := "somePassword"
	dbName := "postgres_limitless"
	plugins := "limitless"

	//connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
	//	host, port, user, password, dbName, plugins,
	//)
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s limitlessWaitForTransactionRouterInfo=false",
		host, port, user, password, dbName, plugins,
	)

	db, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Execute the query
	var result string
	err = db.QueryRow("SELECT aurora_db_instance_identifier()").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}

	// Print the result
	fmt.Println("Query result:", result)
}

func okta_pg() {
	// Database connection string
	host := "someDbHost"
	port := "5432"
	dbUser := "someDbUser"
	dbName := "postgres"
	plugins := "okta"
	iamRegion := "us-east-2"
	idpEndpoint := "dev-12345.okta.com"
	appId := "someAppId"
	iamRoleArn := "arn:aws:iam::12345:role/Okta"
	iamIdpArn := "arn:aws:iam::12345:saml-provider/Okta-cl"
	idpUsername := "someIdpUser"
	idpPassword := "somePassword"

	connStr := fmt.Sprintf("host=%s port=%s dbname=%s plugins=%s dbUser=%s iamRegion=%s idpEndpoint=%s appId=%s iamRoleArn=%s iamIdpArn=%s idpUsername=%s idpPassword=%s iamTokenExpiration=1200",
		host, port, dbName, plugins, dbUser, iamRegion, idpEndpoint, appId, iamRoleArn, iamIdpArn, idpUsername, idpPassword,
	)

	// alternative connection string for pgx
	// connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?plugins=%s&dbUser=%s&iamRegion=%s&idpEndpoint=%s&appId=%s&iamRoleArn=%s&iamIdpArn=%s&idpUsername=%s&idpPassword=%s",
	// 	user, password, host, port, dbName, plugins, dbUser, iamRegion, idpEndpoint, appId, iamRoleArn, iamIdpArn, idpUsername, idpPassword)

	// Open the database connection
	db, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Execute the query
	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}

	// Print the result
	fmt.Println("Query result:", result)
}

func failover() {
	host := "someDbHost"
	port := "5432"
	user := "someUser"
	password := "somePassword"
	dbName := "postgres"
	plugins := "failover"

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
		host, port, user, password, dbName, plugins,
	)

	// alternative connection string for pgx
	// connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?plugins=%s&dbUser=%s&iamRegion=%s&idpEndpoint=%s&appId=%s&iamRoleArn=%s&iamIdpArn=%s&idpUsername=%s&idpPassword=%s",
	// 	user, password, host, port, dbName, plugins, dbUser, iamRegion, idpEndpoint, appId, iamRoleArn, iamIdpArn, idpUsername, idpPassword)

	// Open the database connection
	db, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Execute the query
	var result int
	err = db.QueryRow("SELECT 2").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}

	// Print the result
	fmt.Println("Query result:", result)
}
