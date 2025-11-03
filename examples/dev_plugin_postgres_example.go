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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_simulator_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/error_simulator"
	_ "github.com/aws/aws-advanced-go-wrapper/pgx-driver" // awssql pgx driver
)

type TestErrorCallbackOnSelect1 struct {
}

func (t *TestErrorCallbackOnSelect1) GetErrorToRaise(methodName string, methodArgs ...any) error {
	if methodName == "Conn.QueryContext" {
		argStrings := convertMethodArgsToString(methodArgs...)
		if len(argStrings) == 1 && argStrings[0] == "SELECT 1" {
			return errors.New("test")
		}
	}
	return nil
}

func convertMethodArgsToString(methodArgs ...any) []string {
	args := make([]string, len(methodArgs))
	for i, v := range methodArgs {
		args[i] = fmt.Sprintf("%v", v)
	}
	return args
}

func main() {
	host := "endpoint"
	port := "5432"
	user := "user"
	password := "password"
	dbName := "db"
	plugins := "dev"
	errorToRaise := errors.New("test")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s",
		host, port, user, password, dbName, plugins,
	)

	// Initialize database handler
	db, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Simulate error on Connect
	errSimManager := error_simulator.GetErrorSimulatorManager()
	errSimManager.RaiseErrorOnNextConnect(errorToRaise)

	_, err = db.Conn(context.TODO()) // An error should be returned
	if err != nil {
		// Handle error here
		fmt.Println("We got the following error:", err.Error())
	}

	// Another connection, but should not error out
	conn, err := db.Conn(context.TODO())
	if err != nil {
		log.Fatal("There was an issue opening a connection")
	}
	defer conn.Close()

	// Simulate an error with already opened connection
	errSim, err := error_simulator_util.GetErrorSimulatorFromSqlConn(conn)
	if err != nil {
		log.Fatal("There was an issue getting the connection's error simulator")
	}

	// Note: You can pass in '*' instead of a method name to do all methods
	errSim.RaiseErrorOnNextCall(errorToRaise, "Conn.QueryContext")

	var result int
	err = conn.QueryRowContext(context.TODO(), "SELECT 1").Scan(&result)
	if err != nil {
		// Handle error
		fmt.Println("We got the following error from the query:", err.Error())
	}

	// Subsequent queries should not have errors after it is raised
	err = conn.QueryRowContext(context.TODO(), "SELECT 1").Scan(&result)
	if err != nil {
		log.Fatal("An unexpected error occurred", err)
	}
	fmt.Println("Query result:", result)

	// Check all parameters to decide whether to return an error or not
	errSim.SetCallback(&TestErrorCallbackOnSelect1{})

	// Error should not be raised here since we are doing a select 2
	err = conn.QueryRowContext(context.TODO(), "SELECT 2").Scan(&result)
	if err != nil {
		log.Fatal("An unexpected error occurred", err)
	}
	fmt.Println("Query result:", result)

	// Error will be raised here
	err = conn.QueryRowContext(context.TODO(), "SELECT 1").Scan(&result)
	if err != nil {
		// Handle error
		fmt.Println("We got the following error from the query:", err.Error())
	}
}
