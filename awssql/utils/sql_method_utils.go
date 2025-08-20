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

const SET_AUTOCOMMIT_0 = "set autocommit = 0"

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

	return doesStatementStartTransaction(GetSeparateSqlStatements(query))
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

	return doesStatementCloseTransaction(GetSeparateSqlStatements(query))
}

func GetSeparateSqlStatements(query string) []string {
	statementList := parseMultiStatementQueries(query)
	if len(statementList) == 0 {
		return []string{}
	}

	var statements []string
	for _, statement := range statementList {
		re := regexp.MustCompile(`\s*/\*(.*?)\*/\s*`)
		stmt := strings.TrimSpace(re.ReplaceAllString(statement, " "))
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

func DoesSetReadOnly(query string, dialectFunc func(string) (bool, bool)) (bool, bool) {
	re := regexp.MustCompile(`\s*/\*(.*?)\*/\s*`)
	statements := parseMultiStatementQueries(query)
	for _, statement := range statements {
		cleanStmt := strings.TrimSpace(re.ReplaceAllString(statement, " "))
		if cleanStmt == "" {
			continue
		}
		readOnly, found := dialectFunc(cleanStmt)
		if found {
			return readOnly, true
		}
	}
	return false, false
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
		lowerStatement := strings.ToLower(statement)
		if strings.HasPrefix(lowerStatement, "begin") || strings.HasPrefix(lowerStatement, "start transaction") || lowerStatement == SET_AUTOCOMMIT_0 {
			return true
		}
	}
	return false
}

func doesStatementCloseTransaction(statements []string) bool {
	for _, statement := range statements {
		lowerStatement := strings.ToLower(statement)
		if strings.HasPrefix(lowerStatement, "commit") ||
			strings.HasPrefix(lowerStatement, "rollback") ||
			strings.HasPrefix(lowerStatement, "end") ||
			strings.HasPrefix(lowerStatement, "abort") {
			return true
		}
	}
	return false
}
