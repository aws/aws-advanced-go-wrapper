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

	_ "github.com/aws/aws-advanced-go-wrapper/mysql-driver" // awssql mysql driver
	_ "github.com/aws/aws-advanced-go-wrapper/okta"
)

func okta_mysql() {
	// Database connection string
	host := "endpoint"
	port := "3306"
	dbUser := "someIamUser"
	dbName := "db"
	plugins := "okta"
	iamRegion := "us-east-1"
	idpEndpoint := "123456789.okta.com"
	appId := "abc12345678"
	iamRoleArn := "arn:aws:iam::123456789:role/OktaAccessRole"
	iamIdpArn := "arn:aws:iam::123456789:saml-provider/OktaSAMLIdp"
	idpUsername := "user@example.com"
	idpPassword := "somePassword"

	// Connection parameter allowCleartextPasswords=true must be set for iam connections.
	connStr := fmt.Sprintf("tcp(%s:%s)/%s?plugins=%s&dbUser=%s&iamRegion=%s&idpEndpoint=%s&appId=%s&iamRoleArn=%s&iamIdpArn=%s&idpUsername=%s&idpPassword=%s&allowCleartextPasswords=true",
		host, port, dbName, plugins, dbUser, iamRegion, idpEndpoint, appId, iamRoleArn, iamIdpArn, idpUsername, idpPassword,
	)

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
