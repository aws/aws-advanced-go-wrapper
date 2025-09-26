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
	"database/sql/driver"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/stretchr/testify/assert"
)

func TestResetSessionStateOnCloseFunc(t *testing.T) {
	usedResetStateFunc := false
	resetSessionStateFunc := func(sessionState driver_infrastructure.SessionState, conn driver.Conn) bool {
		usedResetStateFunc = true
		return true
	}
	driver_infrastructure.SetResetSessionStateOnCloseFunc(resetSessionStateFunc)
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	err := sessionStateService.ApplyPristineSessionState(mockConn)
	assert.Nil(t, err)
	assert.True(t, usedResetStateFunc)

	driver_infrastructure.ClearResetSessionStateOnCloseFunc()
	updatedFunc := driver_infrastructure.GetResetSessionStateOnCloseFunc()
	assert.Nil(t, updatedFunc)
}

func TestTransferSessionStateOnCloseFunc(t *testing.T) {
	usedTransferStateFunc := false
	transferSessionStateFunc := func(sessionState driver_infrastructure.SessionState, conn driver.Conn) bool {
		usedTransferStateFunc = true
		return true
	}
	driver_infrastructure.SetTransferSessionStateOnCloseFunc(transferSessionStateFunc)
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	err := sessionStateService.ApplyCurrentSessionState(mockConn)
	assert.Nil(t, err)
	assert.True(t, usedTransferStateFunc)

	driver_infrastructure.ClearTransferSessionStateOnCloseFunc()
	updatedFunc := driver_infrastructure.GetTransferSessionStateOnCloseFunc()
	assert.Nil(t, updatedFunc)
}

func TestGetAndSetAutoCommit(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	assert.Nil(t, sessionStateService.GetAutoCommit())
	sessionStateService.SetAutoCommit(true)
	assert.NotNil(t, sessionStateService.GetAutoCommit())
	assert.True(t, *sessionStateService.GetAutoCommit())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	assert.Nil(t, sessionStateServiceTransferDisabled.GetAutoCommit())
	sessionStateServiceTransferDisabled.SetAutoCommit(true)
	assert.Nil(t, sessionStateServiceTransferDisabled.GetAutoCommit())
}

func TestSetupPristineAutoCommit(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	sessionStateService.SetupPristineAutoCommitWithVal(true)
	assert.NotNil(t, sessionStateService.SessionState.AutoCommit.GetPristineValue())
	assert.True(t, *sessionStateService.SessionState.AutoCommit.GetPristineValue())

	sessionStateService.SetAutoCommit(false)
	sessionStateService.SetupPristineAutoCommit()
	assert.NotNil(t, sessionStateService.SessionState.AutoCommit.GetPristineValue())
	assert.True(t, *sessionStateService.SessionState.AutoCommit.GetPristineValue())

	sessionStateService.SessionState.AutoCommit.Reset()
	sessionStateService.SetAutoCommit(false)
	sessionStateService.SetupPristineAutoCommit()
	assert.NotNil(t, sessionStateService.SessionState.AutoCommit.GetPristineValue())
	assert.False(t, *sessionStateService.SessionState.AutoCommit.GetPristineValue())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	sessionStateService.SetupPristineAutoCommitWithVal(true)
	assert.Nil(t, sessionStateServiceTransferDisabled.SessionState.AutoCommit.GetPristineValue())
	sessionStateService.SessionState.AutoCommit.Reset()
	sessionStateService.SetupPristineAutoCommit()
	assert.Nil(t, sessionStateService.SessionState.AutoCommit.GetPristineValue())
}

func TestGetAndSetReadOnly(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	assert.Nil(t, sessionStateService.GetReadOnly())
	sessionStateService.SetReadOnly(true)
	assert.NotNil(t, sessionStateService.GetReadOnly())
	assert.True(t, *sessionStateService.GetReadOnly())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	assert.Nil(t, sessionStateServiceTransferDisabled.GetReadOnly())
	sessionStateServiceTransferDisabled.SetReadOnly(true)
	assert.Nil(t, sessionStateServiceTransferDisabled.GetReadOnly())
}

func TestSetupPristineReadOnly(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	sessionStateService.SetupPristineReadOnlyWithVal(true)
	assert.NotNil(t, sessionStateService.SessionState.ReadOnly.GetPristineValue())
	assert.True(t, *sessionStateService.SessionState.ReadOnly.GetPristineValue())

	sessionStateService.SetReadOnly(false)
	sessionStateService.SetupPristineReadOnly()
	assert.NotNil(t, sessionStateService.SessionState.ReadOnly.GetPristineValue())
	assert.True(t, *sessionStateService.SessionState.ReadOnly.GetPristineValue())

	sessionStateService.SessionState.ReadOnly.Reset()
	sessionStateService.SetReadOnly(false)
	sessionStateService.SetupPristineReadOnly()
	assert.NotNil(t, sessionStateService.SessionState.ReadOnly.GetPristineValue())
	assert.False(t, *sessionStateService.SessionState.ReadOnly.GetPristineValue())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	sessionStateService.SetupPristineReadOnlyWithVal(true)
	assert.Nil(t, sessionStateServiceTransferDisabled.SessionState.ReadOnly.GetPristineValue())
	sessionStateService.SessionState.ReadOnly.Reset()
	sessionStateService.SetupPristineReadOnly()
	assert.Nil(t, sessionStateService.SessionState.ReadOnly.GetPristineValue())
}

func TestGetAndSetCatalog(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	assert.Nil(t, sessionStateService.GetCatalog())
	sessionStateService.SetCatalog("test")
	assert.NotNil(t, sessionStateService.GetCatalog())
	assert.Equal(t, "test", *sessionStateService.GetCatalog())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	assert.Nil(t, sessionStateServiceTransferDisabled.GetCatalog())
	sessionStateServiceTransferDisabled.SetCatalog("test")
	assert.Nil(t, sessionStateServiceTransferDisabled.GetCatalog())
}

func TestSetupPristineCatalog(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	sessionStateService.SetupPristineCatalogWithVal("test1")
	assert.NotNil(t, sessionStateService.SessionState.Catalog.GetPristineValue())
	assert.Equal(t, "test1", *sessionStateService.SessionState.Catalog.GetPristineValue())

	sessionStateService.SetCatalog("test2")
	sessionStateService.SetupPristineCatalog()
	assert.NotNil(t, sessionStateService.SessionState.Catalog.GetPristineValue())
	assert.Equal(t, "test1", *sessionStateService.SessionState.Catalog.GetPristineValue())

	sessionStateService.SessionState.Catalog.Reset()
	sessionStateService.SetCatalog("test2")
	sessionStateService.SetupPristineCatalog()
	assert.NotNil(t, sessionStateService.SessionState.Catalog.GetPristineValue())
	assert.Equal(t, "test2", *sessionStateService.SessionState.Catalog.GetPristineValue())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	sessionStateService.SetupPristineCatalogWithVal("test1")
	assert.Nil(t, sessionStateServiceTransferDisabled.SessionState.Catalog.GetPristineValue())
	sessionStateService.SessionState.Catalog.Reset()
	sessionStateService.SetupPristineCatalog()
	assert.Nil(t, sessionStateService.SessionState.Catalog.GetPristineValue())
}

func TestGetAndSetSchema(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	assert.Nil(t, sessionStateService.GetSchema())
	sessionStateService.SetSchema("test")
	assert.NotNil(t, sessionStateService.GetSchema())
	assert.Equal(t, "test", *sessionStateService.GetSchema())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	assert.Nil(t, sessionStateServiceTransferDisabled.GetSchema())
	sessionStateServiceTransferDisabled.SetSchema("test")
	assert.Nil(t, sessionStateServiceTransferDisabled.GetSchema())
}

func TestSetupPristineSchema(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	sessionStateService.SetupPristineSchemaWithVal("test1")
	assert.NotNil(t, sessionStateService.SessionState.Schema.GetPristineValue())
	assert.Equal(t, "test1", *sessionStateService.SessionState.Schema.GetPristineValue())

	sessionStateService.SetSchema("test2")
	sessionStateService.SetupPristineSchema()
	assert.NotNil(t, sessionStateService.SessionState.Schema.GetPristineValue())
	assert.Equal(t, "test1", *sessionStateService.SessionState.Schema.GetPristineValue())

	sessionStateService.SessionState.Schema.Reset()
	sessionStateService.SetSchema("test2")
	sessionStateService.SetupPristineSchema()
	assert.NotNil(t, sessionStateService.SessionState.Schema.GetPristineValue())
	assert.Equal(t, "test2", *sessionStateService.SessionState.Schema.GetPristineValue())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	sessionStateService.SetupPristineSchemaWithVal("test1")
	assert.Nil(t, sessionStateServiceTransferDisabled.SessionState.Schema.GetPristineValue())
	sessionStateService.SessionState.Schema.Reset()
	sessionStateService.SetupPristineSchema()
	assert.Nil(t, sessionStateService.SessionState.Schema.GetPristineValue())
}

func TestGetAndSetTxIsolation(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	assert.Nil(t, sessionStateService.GetTransactionIsolation())
	sessionStateService.SetTransactionIsolation(driver_infrastructure.TRANSACTION_REPEATABLE_READ)
	assert.NotNil(t, sessionStateService.GetTransactionIsolation())
	assert.Equal(t, driver_infrastructure.TRANSACTION_REPEATABLE_READ, *sessionStateService.GetTransactionIsolation())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	assert.Nil(t, sessionStateServiceTransferDisabled.GetTransactionIsolation())
	sessionStateServiceTransferDisabled.SetTransactionIsolation(driver_infrastructure.TRANSACTION_REPEATABLE_READ)
	assert.Nil(t, sessionStateServiceTransferDisabled.GetTransactionIsolation())
}

func TestSetupTxIsolation(t *testing.T) {
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, emptyProps)
	sessionStateService.SetupPristineTransactionIsolationWithVal(driver_infrastructure.TRANSACTION_REPEATABLE_READ)
	assert.NotNil(t, sessionStateService.SessionState.TransactionIsolation.GetPristineValue())
	assert.Equal(t, driver_infrastructure.TRANSACTION_REPEATABLE_READ, *sessionStateService.SessionState.TransactionIsolation.GetPristineValue())

	sessionStateService.SetTransactionIsolation(driver_infrastructure.TRANSACTION_SERIALIZABLE)
	sessionStateService.SetupPristineTransactionIsolation()
	assert.NotNil(t, sessionStateService.SessionState.TransactionIsolation.GetPristineValue())
	assert.Equal(t, driver_infrastructure.TRANSACTION_REPEATABLE_READ, *sessionStateService.SessionState.TransactionIsolation.GetPristineValue())

	sessionStateService.SessionState.TransactionIsolation.Reset()
	sessionStateService.SetTransactionIsolation(driver_infrastructure.TRANSACTION_SERIALIZABLE)
	sessionStateService.SetupPristineTransactionIsolation()
	assert.NotNil(t, sessionStateService.SessionState.TransactionIsolation.GetPristineValue())
	assert.Equal(t, driver_infrastructure.TRANSACTION_SERIALIZABLE, *sessionStateService.SessionState.TransactionIsolation.GetPristineValue())

	props := MakeMapFromKeysAndVals(property_util.TRANSFER_SESSION_STATE_ON_SWITCH.Name, "false")
	sessionStateServiceTransferDisabled := driver_infrastructure.NewSessionStateServiceImpl(&plugin_helpers.PluginServiceImpl{}, props)
	sessionStateService.SetupPristineTransactionIsolationWithVal(driver_infrastructure.TRANSACTION_REPEATABLE_READ)
	assert.Nil(t, sessionStateServiceTransferDisabled.SessionState.TransactionIsolation.GetPristineValue())
	sessionStateService.SessionState.TransactionIsolation.Reset()
	sessionStateService.SetupPristineTransactionIsolation()
	assert.Nil(t, sessionStateService.SessionState.TransactionIsolation.GetPristineValue())
}
