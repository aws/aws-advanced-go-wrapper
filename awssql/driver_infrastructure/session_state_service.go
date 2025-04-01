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

import (
	"database/sql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"log/slog"
)

type SessionStateService interface {
	GetAutoCommit() *bool
	SetAutoCommit(bool)
	SetupPristineAutoCommit()
	SetupPristineAutoCommitWithVal(bool)

	GetReadOnly() *bool
	SetReadOnly(bool)
	SetupPristineReadOnly()
	SetupPristineReadOnlyWithVal(bool)

	GetCatalog() *string
	SetCatalog(string)
	SetupPristineCatalog()
	SetupPristineCatalogWithVal(string)

	GetSchema() *string
	SetSchema(string)
	SetupPristineSchema()
	SetupPristineSchemaWithVal(string)

	GetTransactionIsolation() *TransactionIsolationLevel
	SetTransactionIsolation(level TransactionIsolationLevel)
	SetupPristineTransactionIsolation()
	SetupPristineTransactionIsolationWithVal(TransactionIsolationLevel)

	Begin() error
	Complete()
	Reset()

	ApplyCurrentSessionState(conn driver.Conn) error
	ApplyPristineSessionState(conn driver.Conn) error
}

type SessionStateServiceImpl struct {
	SessionState     *SessionState
	copySessionState *SessionState
	pluginService    PluginService
	props            map[string]string
}

func NewSessionStateServiceImpl(pluginService PluginService, props map[string]string) *SessionStateServiceImpl {
	return &SessionStateServiceImpl{
		SessionState:     &SessionState{},
		copySessionState: nil,
		pluginService:    pluginService,
		props:            props,
	}
}

func (sss *SessionStateServiceImpl) transferStateEnabledSetting() bool {
	return property_util.GetVerifiedWrapperPropertyValue[bool](sss.props, property_util.TRANSFER_SESSION_STATE_ON_SWITCH)
}

func (sss *SessionStateServiceImpl) resetStateEnabledSetting() bool {
	return property_util.GetVerifiedWrapperPropertyValue[bool](sss.props, property_util.RESET_SESSION_STATE_ON_CLOSE)
}

func (sss *SessionStateServiceImpl) GetAutoCommit() *bool {
	return sss.SessionState.AutoCommit.GetValue()
}

func (sss *SessionStateServiceImpl) SetAutoCommit(val bool) {
	if !sss.transferStateEnabledSetting() {
		return
	}
	sss.SessionState.AutoCommit.SetValue(val)
}

func (sss *SessionStateServiceImpl) SetupPristineAutoCommit() {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.AutoCommit.GetPristineValue() != nil {
		return
	}

	if sss.GetAutoCommit() == nil {
		sss.SessionState.AutoCommit.ResetPristineValue()
		sss.logCurrentState()
		return
	}

	sss.SessionState.AutoCommit.SetPristineValue(*sss.GetAutoCommit())
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) SetupPristineAutoCommitWithVal(val bool) {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.AutoCommit.GetPristineValue() != nil {
		return
	}

	sss.SessionState.AutoCommit.SetPristineValue(val)
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) GetReadOnly() *bool {
	return sss.SessionState.ReadOnly.GetValue()
}

func (sss *SessionStateServiceImpl) SetReadOnly(val bool) {
	if !sss.transferStateEnabledSetting() {
		return
	}
	sss.SessionState.ReadOnly.SetValue(val)
}

func (sss *SessionStateServiceImpl) SetupPristineReadOnly() {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.ReadOnly.GetPristineValue() != nil {
		return
	}

	if sss.GetReadOnly() == nil {
		sss.SessionState.ReadOnly.ResetPristineValue()
		sss.logCurrentState()
		return
	}

	sss.SessionState.ReadOnly.SetPristineValue(*sss.GetReadOnly())
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) SetupPristineReadOnlyWithVal(val bool) {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.ReadOnly.GetPristineValue() != nil {
		return
	}

	sss.SessionState.ReadOnly.SetPristineValue(val)
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) GetCatalog() *string {
	return sss.SessionState.Catalog.GetValue()
}

func (sss *SessionStateServiceImpl) SetCatalog(val string) {
	if !sss.transferStateEnabledSetting() {
		return
	}
	sss.SessionState.Catalog.SetValue(val)
}

func (sss *SessionStateServiceImpl) SetupPristineCatalog() {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.Catalog.GetPristineValue() != nil {
		return
	}

	if sss.GetCatalog() == nil {
		sss.SessionState.Catalog.ResetPristineValue()
		sss.logCurrentState()
		return
	}

	sss.SessionState.Catalog.SetPristineValue(*sss.GetCatalog())
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) SetupPristineCatalogWithVal(val string) {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.Catalog.GetPristineValue() != nil {
		return
	}

	sss.SessionState.Catalog.SetPristineValue(val)
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) GetSchema() *string {
	return sss.SessionState.Schema.GetValue()
}

func (sss *SessionStateServiceImpl) SetSchema(val string) {
	if !sss.transferStateEnabledSetting() {
		return
	}
	sss.SessionState.Schema.SetValue(val)
}

func (sss *SessionStateServiceImpl) SetupPristineSchema() {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.Schema.GetPristineValue() != nil {
		return
	}

	if sss.GetSchema() == nil {
		sss.SessionState.Schema.ResetPristineValue()
		sss.logCurrentState()
		return
	}

	sss.SessionState.Schema.SetPristineValue(*sss.GetSchema())
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) SetupPristineSchemaWithVal(val string) {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.Schema.GetPristineValue() != nil {
		return
	}

	sss.SessionState.Schema.SetPristineValue(val)
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) GetTransactionIsolation() *TransactionIsolationLevel {
	return sss.SessionState.TransactionIsolation.GetValue()
}

func (sss *SessionStateServiceImpl) SetTransactionIsolation(val TransactionIsolationLevel) {
	if !sss.transferStateEnabledSetting() {
		return
	}
	sss.SessionState.TransactionIsolation.SetValue(val)
}

func (sss *SessionStateServiceImpl) SetupPristineTransactionIsolation() {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.TransactionIsolation.GetPristineValue() != nil {
		return
	}

	if sss.GetTransactionIsolation() == nil {
		sss.SessionState.TransactionIsolation.ResetPristineValue()
		sss.logCurrentState()
		return
	}

	sss.SessionState.TransactionIsolation.SetPristineValue(*sss.GetTransactionIsolation())
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) SetupPristineTransactionIsolationWithVal(val TransactionIsolationLevel) {
	if !sss.transferStateEnabledSetting() {
		return
	}

	if sss.SessionState.TransactionIsolation.GetPristineValue() != nil {
		return
	}

	sss.SessionState.TransactionIsolation.SetPristineValue(val)
	sss.logCurrentState()
}

func (sss *SessionStateServiceImpl) ApplyCurrentSessionState(newConn driver.Conn) error {
	if !sss.transferStateEnabledSetting() {
		return nil
	}

	transferSessionStateFunc := GetTransferSessionStateOnCloseFunc()
	if transferSessionStateFunc != nil && sss.SessionState != nil {
		isHandled := transferSessionStateFunc(*sss.SessionState, newConn)
		if isHandled {
			// Custom function has handled session transfer.
			return nil
		}
	}

	if sss.SessionState.AutoCommit.GetValue() != nil {
		sss.SessionState.AutoCommit.ResetPristineValue()
		sss.SetupPristineAutoCommit()
		query, err := sss.pluginService.GetDialect().GetSetAutoCommitQuery(*sss.SessionState.AutoCommit.GetValue())
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(newConn, query)
		if err != nil {
			return err
		}
	}

	if sss.SessionState.ReadOnly.GetValue() != nil {
		sss.SessionState.ReadOnly.ResetPristineValue()
		sss.SetupPristineReadOnly()
		query, err := sss.pluginService.GetDialect().GetSetReadOnlyQuery(*sss.SessionState.ReadOnly.GetValue())
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(newConn, query)
		if err != nil {
			return err
		}
	}

	if sss.SessionState.Catalog.GetValue() != nil {
		sss.SessionState.Catalog.ResetPristineValue()
		sss.SetupPristineCatalog()
		query, err := sss.pluginService.GetDialect().GetSetCatalogQuery(*sss.SessionState.Catalog.GetValue())
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(newConn, query)
		if err != nil {
			return err
		}
	}

	if sss.SessionState.Schema.GetValue() != nil {
		sss.SessionState.Schema.ResetPristineValue()
		sss.SetupPristineSchema()
		query, err := sss.pluginService.GetDialect().GetSetSchemaQuery(*sss.SessionState.Schema.GetValue())
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(newConn, query)
		if err != nil {
			return err
		}
	}

	if sss.SessionState.TransactionIsolation.GetValue() != nil {
		sss.SessionState.TransactionIsolation.ResetPristineValue()
		sss.SetupPristineTransactionIsolation()
		query, err := sss.pluginService.GetDialect().GetSetTransactionIsolationQuery(*sss.SessionState.TransactionIsolation.GetValue())
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(newConn, query)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sss *SessionStateServiceImpl) ApplyPristineSessionState(conn driver.Conn) error {
	if !sss.resetStateEnabledSetting() {
		return nil
	}

	resetSessionStateFunc := GetResetSessionStateOnCloseFunc()
	if resetSessionStateFunc != nil && sss.SessionState != nil {
		isHandled := resetSessionStateFunc(*sss.SessionState, conn)
		if isHandled {
			// Custom function has handled session transfer.
			return nil
		}
	}

	if sss.copySessionState == nil {
		return nil
	}

	// Set pristine states on all target client.
	// The states that will be set are: autoCommit, readonly, schema, catalog, transactionIsolation.
	if sss.copySessionState.AutoCommit.CanRestorePristine() && sss.copySessionState.AutoCommit.GetPristineValue() != nil {
		pristineAutoCommit := sss.copySessionState.AutoCommit.GetPristineValue()
		query, err := sss.pluginService.GetDialect().GetSetAutoCommitQuery(*pristineAutoCommit)
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(conn, query)
		if err != nil {
			return err
		}
	}

	if sss.copySessionState.ReadOnly.CanRestorePristine() && sss.copySessionState.ReadOnly.GetPristineValue() != nil {
		pristineReadOnly := sss.copySessionState.ReadOnly.GetPristineValue()
		query, err := sss.pluginService.GetDialect().GetSetReadOnlyQuery(*pristineReadOnly)
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(conn, query)
		if err != nil {
			return err
		}
	}

	if sss.copySessionState.Catalog.CanRestorePristine() && sss.copySessionState.Catalog.GetPristineValue() != nil {
		pristineCatalog := sss.copySessionState.Catalog.GetPristineValue()
		query, err := sss.pluginService.GetDialect().GetSetCatalogQuery(*pristineCatalog)
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(conn, query)
		if err != nil {
			return err
		}
	}

	if sss.copySessionState.Schema.CanRestorePristine() && sss.copySessionState.Schema.GetPristineValue() != nil {
		pristineSchema := sss.copySessionState.Schema.GetPristineValue()
		query, err := sss.pluginService.GetDialect().GetSetSchemaQuery(*pristineSchema)
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(conn, query)
		if err != nil {
			return err
		}
	}

	if sss.copySessionState.TransactionIsolation.CanRestorePristine() && sss.copySessionState.TransactionIsolation.GetPristineValue() != nil {
		pristineTransactionIsolation := sss.copySessionState.TransactionIsolation.GetPristineValue()
		query, err := sss.pluginService.GetDialect().GetSetTransactionIsolationQuery(*pristineTransactionIsolation)
		if err != nil {
			return err
		}
		err = utils.ExecQueryDirectly(conn, query)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sss *SessionStateServiceImpl) Begin() error {
	sss.logCurrentState()

	if !sss.transferStateEnabledSetting() && !sss.resetStateEnabledSetting() {
		return nil
	}

	if sss.copySessionState != nil {
		return error_util.NewGenericAwsWrapperError("SessionStateService.transferIncomplete")
	}

	sss.copySessionState = sss.SessionState.Copy()
	return nil
}

func (sss *SessionStateServiceImpl) Complete() {
	sss.copySessionState = nil
}

func (sss *SessionStateServiceImpl) Reset() {
	if sss.SessionState != nil {
		sss.SessionState.AutoCommit.Reset()
		sss.SessionState.ReadOnly.Reset()
		sss.SessionState.Catalog.Reset()
		sss.SessionState.Schema.Reset()
		sss.SessionState.TransactionIsolation.Reset()
	}
}

func (sss *SessionStateServiceImpl) logCurrentState() {
	slog.Debug(error_util.GetMessage("SessionStateService.logState", sss.SessionState.ToString()))
}
