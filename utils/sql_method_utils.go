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

package utils

import (
	"regexp"
	"strings"
)

const SET_AUTOCOMMIT_0 = "SET AUTOCOMMIT = 0"

func DoesOpenTransaction(methodName string, methodArgs ...any) bool {
	if methodName == CONN_BEGIN_TX ||
		methodName == CONN_BEGIN {
		return true
	}
	if methodArgs == nil {
		return false
	}
	query, ok := methodArgs[0].(string)
	if !ok || len(query) == 0 {
		return false
	}

	return doesStatementStartTransaction(getSeparateSqlStatements(query))
}

func DoesCloseTransaction(methodName string, methodArgs ...any) bool {
	if methodName == TX_ROLLBACK ||
		methodName == TX_COMMIT {
		return true
	}

	if methodArgs == nil {
		return false
	}
	query, ok := methodArgs[0].(string)
	if !ok {
		return false
	}

	return doesStatementCloseTransaction(getSeparateSqlStatements(query))
}

func getSeparateSqlStatements(query string) []string {
	statementList := parseMultiStatementQueries(query)
	if len(statementList) == 0 {
		return []string{}
	}

	statements := make([]string, len(statementList))
	for i, statement := range statementList {
		statement = strings.ToUpper(statement)
		re := regexp.MustCompile(`\s*/\*(.*?)\*/\s*`)
		statement = strings.TrimSpace(re.ReplaceAllString(statement, " "))
		statements[i] = statement
	}
	return statements
}

func parseMultiStatementQueries(query string) []string {
	if query == "" {
		return []string{}
	}

	re := regexp.MustCompile(`\s+`)
	query = re.ReplaceAllString(query, " ")

	// Check to see if the string only has blank spaces.
	query = strings.TrimSpace(query)
	if query == "" {
		return []string{}
	}

	return strings.Split(query, ";")
}

func doesStatementStartTransaction(statements []string) bool {
	for _, statement := range statements {
		if strings.HasPrefix(statement, "BEGIN") || strings.HasPrefix(statement, "START TRANSACTION") || statement == SET_AUTOCOMMIT_0 {
			return true
		}
	}
	return false
}

func doesStatementCloseTransaction(statements []string) bool {
	for _, statement := range statements {
		if strings.HasPrefix(statement, "COMMIT") ||
			strings.HasPrefix(statement, "ROLLBACK") ||
			strings.HasPrefix(statement, "END") ||
			strings.HasPrefix(statement, "ABORT") {
			return true
		}
	}
	return false
}
