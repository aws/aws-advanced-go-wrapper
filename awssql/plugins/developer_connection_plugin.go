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

package plugins

import (
	"database/sql/driver"
	"log/slog"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/error_simulator"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type DeveloperConnectionPluginFactory struct {
}

func NewDeveloperConnectionPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return &DeveloperConnectionPluginFactory{}
}

func (d *DeveloperConnectionPluginFactory) GetInstance(
	servicesContainer driver_infrastructure.ServicesContainer, props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewDeveloperConnectionPlugin(servicesContainer.GetPluginService(), props), nil
}

func (d *DeveloperConnectionPluginFactory) ClearCaches() {
}

type DeveloperConnectionPlugin struct {
	BaseConnectionPlugin
	errorSimulatorMethodCallback error_simulator.ErrorSimulatorMethodCallback
	props                        *utils.RWMap[string, string]
	nextMethodName               string
	nextError                    error
	pluginService                driver_infrastructure.PluginService
}

func NewDeveloperConnectionPlugin(pluginService driver_infrastructure.PluginService, props *utils.RWMap[string, string]) driver_infrastructure.ConnectionPlugin {
	return &DeveloperConnectionPlugin{
		pluginService: pluginService,
		props:         props,
	}
}

func (d *DeveloperConnectionPlugin) GetPluginCode() string {
	return driver_infrastructure.DEVELOPER_PLUGIN_CODE
}

func (d *DeveloperConnectionPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.ALL_METHODS}
}

func (d *DeveloperConnectionPlugin) RaiseErrorOnNextCall(err error, methodName string) {
	d.nextError = err
	if methodName != "" {
		d.nextMethodName = methodName
	} else {
		d.nextMethodName = plugin_helpers.ALL_METHODS
	}
}

func (d *DeveloperConnectionPlugin) SetCallback(errorSimulatorMethodCallback error_simulator.ErrorSimulatorMethodCallback) {
	d.errorSimulatorMethodCallback = errorSimulatorMethodCallback
}

func (d *DeveloperConnectionPlugin) Execute(
	_ driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	err := d.raiseErrorIfNeeded(methodName, methodArgs...)
	if err != nil {
		wrappedErr = err
		return
	}
	return executeFunc()
}

func (d *DeveloperConnectionPlugin) raiseErrorIfNeeded(methodName string, methodArgs ...any) error {
	if d.nextError != nil {
		if plugin_helpers.ALL_METHODS == d.nextMethodName || methodName == d.nextMethodName {
			return d.raiseError(d.nextError, methodName)
		}
	} else if d.errorSimulatorMethodCallback != nil {
		return d.raiseError(d.errorSimulatorMethodCallback.GetErrorToRaise(methodName, methodArgs...), methodName)
	}
	return nil
}

func (d *DeveloperConnectionPlugin) raiseError(err error, methodName string) error {
	if err == nil {
		return nil
	}
	d.nextError = nil
	d.nextMethodName = ""

	slog.Debug(error_util.GetMessage("DeveloperPlugin.raisedErrorOnMethod", err.Error(), methodName))
	return err
}

func (d *DeveloperConnectionPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	err := d.raiseErrorOnConnectIfNeeded(hostInfo, props, isInitialConnection)

	if err != nil {
		return nil, err
	}
	return connectFunc(props)
}

func (d *DeveloperConnectionPlugin) raiseErrorOnConnectIfNeeded(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool) error {
	errorSimulatorManager := error_simulator.GetErrorSimulatorManager()

	if errorSimulatorManager.NextError != nil {
		return d.raiseErrorOnConnect(error_simulator.GetErrorSimulatorManager().NextError)
	} else if errorSimulatorManager.ConnectCallback != nil {
		return d.raiseErrorOnConnect(errorSimulatorManager.ConnectCallback.GetErrorToRaise(hostInfo, props.GetAllEntries(), isInitialConnection))
	}

	return nil
}

func (d *DeveloperConnectionPlugin) raiseErrorOnConnect(err error) error {
	if err == nil {
		return nil
	}

	error_simulator.GetErrorSimulatorManager().NextError = nil
	slog.Debug(error_util.GetMessage("DeveloperPlugin.raisedErrorOnConnect", err.Error()))

	return err
}
