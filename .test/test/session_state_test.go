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

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/stretchr/testify/assert"
)

func TestCopySessionState(t *testing.T) {
	originalState := driver_infrastructure.SessionState{}
	originalState.AutoCommit.SetValue(true)
	originalState.ReadOnly.SetValue(true)
	originalState.Catalog.SetValue("catalog")
	originalState.Schema.SetValue("schema")
	originalState.TransactionIsolation.SetValue(driver_infrastructure.TRANSACTION_REPEATABLE_READ)

	copyState := originalState.Copy()
	assert.Equal(t, *originalState.AutoCommit.GetValue(), *copyState.AutoCommit.GetValue())
	assert.Equal(t, *originalState.ReadOnly.GetValue(), *copyState.ReadOnly.GetValue())
	assert.Equal(t, *originalState.Catalog.GetValue(), *copyState.Catalog.GetValue())
	assert.Equal(t, *originalState.Schema.GetValue(), *copyState.Schema.GetValue())
	assert.Equal(t, *originalState.TransactionIsolation.GetValue(), *copyState.TransactionIsolation.GetValue())
	assert.Equal(t, originalState.ToString(), copyState.ToString())
}

func TestCanRestorePristine(t *testing.T) {
	originalState := driver_infrastructure.SessionState{}
	// Value and pristine value are nil
	assert.Equal(t, false, originalState.AutoCommit.CanRestorePristine())

	// Value is nil
	originalState.AutoCommit.SetPristineValue(true)
	assert.Equal(t, true, originalState.AutoCommit.CanRestorePristine())

	// Value and pristine value are not nil, and they are the same
	originalState.AutoCommit.SetValue(true)
	assert.Equal(t, false, originalState.AutoCommit.CanRestorePristine())

	// Value and pristine value are not nil, and they are not the same
	originalState.AutoCommit.SetValue(false)
	assert.Equal(t, true, originalState.AutoCommit.CanRestorePristine())
}
