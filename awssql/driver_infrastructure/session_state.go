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

package driver_infrastructure

import "fmt"

type SessionStateField[T comparable] struct {
	value         *T
	pristineValue *T
}

func (ssf *SessionStateField[T]) GetValue() *T {
	return ssf.value
}

func (ssf *SessionStateField[T]) SetValue(value T) {
	ssf.value = &value
}

func (ssf *SessionStateField[T]) ResetValue() {
	ssf.value = nil
}

func (ssf *SessionStateField[T]) GetPristineValue() *T {
	return ssf.pristineValue
}

func (ssf *SessionStateField[T]) SetPristineValue(value T) {
	ssf.pristineValue = &value
}

func (ssf *SessionStateField[T]) ResetPristineValue() {
	ssf.pristineValue = nil
}

func (ssf *SessionStateField[T]) Reset() {
	ssf.ResetValue()
	ssf.ResetPristineValue()
}

func (ssf *SessionStateField[T]) CanRestorePristine() bool {
	if ssf.pristineValue == nil {
		return false
	}

	if ssf.value != nil {
		// It's necessary to restore the pristine value only if the current session value is not the same as the pristine value.
		return *ssf.value != *ssf.pristineValue
	}

	// It's inconclusive if the current value is the same as pristine value, so we need to take the safest path.
	return true
}

type SessionState struct {
	AutoCommit           SessionStateField[bool]
	ReadOnly             SessionStateField[bool]
	Catalog              SessionStateField[string]
	Schema               SessionStateField[string]
	TransactionIsolation SessionStateField[TransactionIsolationLevel]
}

func (ssf *SessionState) Copy() *SessionState {
	autoCommit := ssf.AutoCommit.GetValue()
	readOnly := ssf.ReadOnly.GetValue()
	catalog := ssf.Catalog.GetValue()
	schema := ssf.Schema.GetValue()
	transactionIsolation := ssf.TransactionIsolation.GetValue()

	newSessionState := &SessionState{}
	if autoCommit != nil {
		newSessionState.AutoCommit.SetValue(*autoCommit)
	}
	if readOnly != nil {
		newSessionState.ReadOnly.SetValue(*readOnly)
	}
	if catalog != nil {
		newSessionState.Catalog.SetValue(*catalog)
	}
	if schema != nil {
		newSessionState.Schema.SetValue(*schema)
	}
	if transactionIsolation != nil {
		newSessionState.TransactionIsolation.SetValue(*transactionIsolation)
	}

	return newSessionState
}

func (ssf *SessionState) ToString() string {
	var autoCommit bool
	if ssf.AutoCommit.GetValue() != nil {
		autoCommit = *ssf.AutoCommit.GetValue()
	}
	var readOnly bool
	if ssf.ReadOnly.GetValue() != nil {
		readOnly = *ssf.ReadOnly.GetValue()
	}
	var catalog string
	if ssf.Catalog.GetValue() != nil {
		catalog = *ssf.Catalog.GetValue()
	}
	var schema string
	if ssf.Schema.GetValue() != nil {
		schema = *ssf.Schema.GetValue()
	}
	var txIsolation TransactionIsolationLevel
	if ssf.TransactionIsolation.GetValue() != nil {
		txIsolation = *ssf.TransactionIsolation.GetValue()
	}
	return fmt.Sprintf(
		"autocommit: %v\n"+
			"readOnly: %v\n"+
			"catalog: %v\n"+
			"schema: %v\n"+
			"transactionIsolation: %v\n",
		autoCommit,
		readOnly,
		catalog,
		schema,
		txIsolation,
	)
}
