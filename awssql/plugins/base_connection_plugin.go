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
	"fmt"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

type BaseConnectionPlugin struct {
}

func (b BaseConnectionPlugin) GetSubscribedMethods() []string {
	return []string{}
}

func (b BaseConnectionPlugin) Execute(
	connInvokedOn driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	return executeFunc()
}

func (b BaseConnectionPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return connectFunc()
}

func (b BaseConnectionPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return connectFunc()
}

func (b BaseConnectionPlugin) AcceptsStrategy(strategy string) bool {
	return false
}

func (b BaseConnectionPlugin) GetHostInfoByStrategy(role host_info_util.HostRole, strategy string, hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	return nil, error_util.NewUnsupportedMethodError("GetHostInfoByStrategy", fmt.Sprintf("%T", b))
}

func (b BaseConnectionPlugin) GetHostSelectorStrategy(strategy string) (driver_infrastructure.HostSelector, error) {
	return nil, error_util.NewUnsupportedMethodError("GetHostSelectorStrategy", fmt.Sprintf("%T", b))
}

func (b BaseConnectionPlugin) NotifyConnectionChanged(changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	return driver_infrastructure.NO_OPINION
}

func (b BaseConnectionPlugin) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	// Do nothing.
}

func (b BaseConnectionPlugin) InitHostProvider(
	initialUrl string,
	props map[string]string,
	hostListProviderService driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	return initHostProviderFunc()
}
