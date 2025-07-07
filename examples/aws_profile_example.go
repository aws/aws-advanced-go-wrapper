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

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	_ "github.com/aws/aws-advanced-go-wrapper/iam"
	_ "github.com/aws/aws-advanced-go-wrapper/pgx-driver" // awssql pgx driver
)

func custom_aws_profile_example() {
	// Database connection string
	host := "endpoint"
	port := "5432"
	iamUser := "user"
	dbName := "db"
	plugins := "iam"
	iamRegion := "us-east-1"

	// The profile to use in .aws/credentials file
	awsProfile := "customprofile"

	// setup custom credentials provider
	auth_helpers.SetAwsCredentialsProviderHandler(CustomAwsCredentialsProvider{})

	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s plugins=%s iamRegion=%s awsProfile=%s",
		host, port, iamUser, dbName, plugins, iamRegion, awsProfile,
	)

	// alternative connection string for pgx
	// connStr := fmt.Sprintf("postgresql://%s@%s:%s/%s?plugins=%s&iamRegion=%s&wasProfile=%s",
	// 	user, host, port, dbName, plugins, iamRegion, awsProfile)

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
