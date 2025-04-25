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
	"awssql/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRollbackWithCurrentTx(t *testing.T) {
	mockConn := MockConn{}
	mockTx := NewMockTx()

	utils.Rollback(&mockConn, mockTx)
	assert.Equal(t, 1, *mockTx.rollbackCounter)
	assert.Equal(t, 0, mockConn.execContextCounter)
}

func TestRollbackWithNilCurrentTx(t *testing.T) {
	mockConn := MockConn{}

	utils.Rollback(&mockConn, nil)
	assert.Equal(t, 1, mockConn.execContextCounter)
}
