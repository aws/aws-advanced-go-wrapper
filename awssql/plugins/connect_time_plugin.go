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
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type ConnectTimePluginFactory struct{}

func NewConnectTimePluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return ConnectTimePluginFactory{}
}

func (factory ConnectTimePluginFactory) GetInstance(servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
) (driver_infrastructure.ConnectionPlugin, error) {
	return NewConnectTimePlugin(servicesContainer.GetPluginService(), props)
}

func (factory ConnectTimePluginFactory) ClearCaches() {}

type ConnectTimePlugin struct {
	BaseConnectionPlugin
	connectTimeNano int64
}

func NewConnectTimePlugin(_ driver_infrastructure.PluginService,
	_ *utils.RWMap[string, string]) (*ConnectTimePlugin, error) {
	return &ConnectTimePlugin{}, nil
}

func (d *ConnectTimePlugin) GetPluginCode() string {
	return driver_infrastructure.CONNECT_TIME_PLUGIN_CODE
}

func (d *ConnectTimePlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD, plugin_helpers.FORCE_CONNECT_METHOD}
}

func (d *ConnectTimePlugin) Connect(
	_ *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	start := time.Now()
	conn, err := connectFunc(props)
	elapsed := time.Since(start)

	d.connectTimeNano += elapsed.Nanoseconds()
	slog.Debug(error_util.GetMessage("ConnectTimePlugin.connectTime", elapsed.Milliseconds()))
	return conn, err
}

func (d *ConnectTimePlugin) ForceConnect(
	_ *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	forceConnectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	start := time.Now()
	conn, err := forceConnectFunc(props)
	elapsed := time.Since(start)

	d.connectTimeNano += elapsed.Nanoseconds()
	slog.Debug(error_util.GetMessage("ConnectTimePlugin.forceConnectTime", elapsed.Milliseconds()))
	return conn, err
}

func (d *ConnectTimePlugin) ResetConnectTime() {
	d.connectTimeNano = 0
}

func (d *ConnectTimePlugin) GetTotalConnectTime() int64 {
	return d.connectTimeNano
}
