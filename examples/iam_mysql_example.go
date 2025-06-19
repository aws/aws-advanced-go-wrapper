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
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"

	_ "github.com/aws/aws-advanced-go-wrapper/iam"
	_ "github.com/aws/aws-advanced-go-wrapper/mysql-driver" // awssql mysql driver
	"github.com/go-sql-driver/mysql"
)

func iam_mysql() {
	// Database connection string
	host := "endpoint"
	port := "3306"
	iamUser := "user"
	dbName := "db"
	plugins := "iam"
	iamRegion := "us-east-1"
	// To use custom tls.Config set to the name of registered config. Ex. "aws-rds-secure".
	tlsConfig := "true"

	// Connection parameter allowCleartextPasswords=true must be set for iam connections.
	connStr := fmt.Sprintf("%s@tcp(%s:%s)/%s?plugins=%s&iamRegion=%s&allowCleartextPasswords=true&tls=%s",
		iamUser, host, port, dbName, plugins, iamRegion, tlsConfig,
	)

	// If using a custom tls.Config, ensure it is set up and registered before connecting. Example in setupSSL().

	// Open the database connection
	db, err := sql.Open("awssql-mysql", connStr)
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

func setupSSL() {
	caCert, err := ioutil.ReadFile("/path/to/certificate.pem")
	if err != nil {
		// handle error
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		// handle error
	}

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	mysql.RegisterTLSConfig("aws-rds-secure", tlsConfig)
}
