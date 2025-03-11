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

package driver

import (
	"database/sql/driver"
)

type ConnectionPlugin interface {
	GetSubscribedMethods() []string
	Execute(methodName string, executeFunc ExecuteFunc, methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error)
	Connect(hostInfo HostInfo, properties map[string]any, isInitialConnection bool, connectFunc ConnectFunc) (*driver.Conn, error)
	ForceConnect(hostInfo HostInfo, properties map[string]any, isInitialConnection bool, connectFunc ConnectFunc) (*driver.Conn, error)
	AcceptsStrategy(role HostRole, strategy string) bool
	GetHostInfoByStrategy(role HostRole, strategy string, hosts []HostInfo) (HostInfo, error)
	NotifyConnectionChanged() OldConnectionSuggestedAction
	NotifyHostListChanged(changes map[HostChangeOptions]bool)
	InitHostProvider(initialUrl string, props map[string]any, hostListProviderService HostListProviderService, initHostProviderFunc func())
}
