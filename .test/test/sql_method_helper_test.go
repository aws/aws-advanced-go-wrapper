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
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/stretchr/testify/assert"
	"testing"
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
