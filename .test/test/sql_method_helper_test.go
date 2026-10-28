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

package test

import (
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/stretchr/testify/assert"
)

func TestOpenTransaction(t *testing.T) {
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "start transaction;"))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "START TRANSACTION"))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "  bEgIn ; "))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, " select 1; bEgIn ; "))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "START /* COMMENT */ TRANSACTION; SELECT 1;"))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "START/* COMMENT */TRANSACTION;"))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "START      /* COMMENT */    TRANSACTION;"))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "START   /*COMMENT*/TRANSACTION;"))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "/*COMMENT*/START   /*COMMENT*/TRANSACTION;"))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, " /*COMMENT*/ START   /*COMMENT*/TRANSACTION;"))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, " /*COMMENT*/ begin"))
	assert.Equal(t, false, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, "commit"))
	assert.Equal(t, false, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, " select 1"))
	assert.Equal(t, false, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, " INSERT INTO test_table VALUES (1) ; "))
	assert.Equal(t, false, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, " set autocommit = 1 "))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, " set autocommit = 0 "))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, " set   autocommit    =    0 "))
	assert.Equal(t, false, utils.DoesOpenTransaction(utils.CONN_QUERY_CONTEXT, ""))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_BEGIN, ""))
	assert.Equal(t, true, utils.DoesOpenTransaction(utils.CONN_BEGIN_TX, ""))
}

func TestCloseTransaction(t *testing.T) {
	assert.Equal(t, true, utils.DoesCloseTransaction(utils.CONN_QUERY_CONTEXT, "rollback;"))
	assert.Equal(t, true, utils.DoesCloseTransaction(utils.CONN_QUERY_CONTEXT, "commit;"))
	assert.Equal(t, true, utils.DoesCloseTransaction(utils.CONN_QUERY_CONTEXT, "end"))
	assert.Equal(t, true, utils.DoesCloseTransaction(utils.CONN_QUERY_CONTEXT, "abort;"))
	assert.Equal(t, false, utils.DoesCloseTransaction(utils.CONN_QUERY_CONTEXT, "select 1"))
	assert.Equal(t, true, utils.DoesCloseTransaction(utils.TX_COMMIT, ""))
	assert.Equal(t, true, utils.DoesCloseTransaction(utils.TX_ROLLBACK, ""))
}

func TestGetTransactionBoundaryFromMethodName(t *testing.T) {
	opens, closes := utils.GetTransactionBoundary(utils.CONN_BEGIN, nil)
	assert.True(t, opens)
	assert.False(t, closes)

	opens, closes = utils.GetTransactionBoundary(utils.CONN_BEGIN_TX, nil)
	assert.True(t, opens)
	assert.False(t, closes)

	opens, closes = utils.GetTransactionBoundary(utils.TX_COMMIT, nil)
	assert.False(t, opens)
	assert.True(t, closes)

	opens, closes = utils.GetTransactionBoundary(utils.TX_ROLLBACK, nil)
	assert.False(t, opens)
	assert.True(t, closes)

	// A transaction method decides the boundary on its own; its arguments are not parsed as SQL.
	opens, closes = utils.GetTransactionBoundary(utils.CONN_BEGIN, nil, "commit")
	assert.True(t, opens)
	assert.False(t, closes)
}

func TestGetTransactionBoundaryFromQuery(t *testing.T) {
	tests := []struct {
		name   string
		args   []any
		opens  bool
		closes bool
	}{
		{"opens", []any{"begin"}, true, false},
		{"closes", []any{"commit"}, false, true},
		{"opens and closes", []any{"begin; select 1; commit"}, true, true},
		{"neither", []any{"select 1"}, false, false},
		{"comment only", []any{"/* begin */ select 1"}, false, false},
		{"empty query", []any{""}, false, false},
		{"non-string argument", []any{42}, false, false},
		{"no arguments", nil, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opens, closes := utils.GetTransactionBoundary(utils.CONN_QUERY_CONTEXT, nil, tt.args...)
			assert.Equal(t, tt.opens, opens)
			assert.Equal(t, tt.closes, closes)
		})
	}
}

func TestGetTransactionBoundaryUsesMatchingAnalysis(t *testing.T) {
	// The analysis disagrees with the query text, so a true result shows it was used instead of reparsing.
	analysis := &utils.SqlStatementAnalysis{Query: "select 1", OpensTransaction: true, ClosesTransaction: true}

	opens, closes := utils.GetTransactionBoundary(utils.CONN_QUERY_CONTEXT, analysis, "select 1")

	assert.True(t, opens)
	assert.True(t, closes)
}

func TestGetTransactionBoundaryIgnoresAnalysisOfAnotherQuery(t *testing.T) {
	analysis := &utils.SqlStatementAnalysis{Query: "begin", OpensTransaction: true}

	opens, closes := utils.GetTransactionBoundary(utils.CONN_QUERY_CONTEXT, analysis, "select 1")

	assert.False(t, opens)
	assert.False(t, closes)
}

func TestAnalyzeStatements(t *testing.T) {
	query := "begin; update t set a = 1; commit"

	analysis := utils.AnalyzeStatements(query, utils.GetSeparateSqlStatements(query))

	assert.Equal(t, query, analysis.Query)
	assert.True(t, analysis.OpensTransaction)
	assert.True(t, analysis.ClosesTransaction)
}

func TestGetSeparateSqlStatementsStripsComments(t *testing.T) {
	assert.Equal(t, []string{"select 1", "begin"}, utils.GetSeparateSqlStatements("  select 1 ; begin  "))
	assert.Equal(t, []string{"select 1", "begin"}, utils.GetSeparateSqlStatements("select /* a */ 1; /* b */ begin"))
	assert.Equal(t, []string{"select 1"}, utils.GetSeparateSqlStatements("select 1; /* only a comment */"))
}
