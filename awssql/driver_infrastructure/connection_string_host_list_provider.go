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
	"log/slog"
	"math"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type ConnectionStringHostListProvider struct {
	isSingleWriterConnectionString bool
	props                          *utils.RWMap[string, string]
	hostListProviderService        HostListProviderService
	isInitialized                  bool
	hostList                       []*host_info_util.HostInfo
	initialHost                    string
}

func NewConnectionStringHostListProvider(props *utils.RWMap[string, string], hostListProviderService HostListProviderService) *ConnectionStringHostListProvider {
	isSingleWriterConnectionString := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.SINGLE_WRITER_DSN)
	initialHost := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.HOST)
	return &ConnectionStringHostListProvider{
		isSingleWriterConnectionString,
		props,
		hostListProviderService,
		false,
		[]*host_info_util.HostInfo{},
		initialHost,
	}
}

func (c *ConnectionStringHostListProvider) init() error {
	if c.isInitialized {
		return nil
	}

	hosts, err := property_util.GetHostsFromProps(c.props, c.isSingleWriterConnectionString)
	if err != nil {
		return err
	}
	c.hostList = append(c.hostList, hosts...)

	if len(c.hostList) == 0 {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("ConnectionStringHostListProvider.parsedListEmpty"))
	}

	c.hostListProviderService.SetInitialConnectionHostInfo(c.hostList[0])
	c.isInitialized = true
	return nil
}

func (c *ConnectionStringHostListProvider) IsStaticHostListProvider() bool {
	return true
}

func (c *ConnectionStringHostListProvider) Refresh() ([]*host_info_util.HostInfo, error) {
	err := c.init()
	return host_info_util.CopyHostList(c.hostList), err
}

func (c *ConnectionStringHostListProvider) ForceRefresh() ([]*host_info_util.HostInfo, error) {
	err := c.init()
	return host_info_util.CopyHostList(c.hostList), err
}

func (c *ConnectionStringHostListProvider) ForceRefreshWithOptions(verifyTopology bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	err := c.init()
	return host_info_util.CopyHostList(c.hostList), err
}

func (c *ConnectionStringHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	panic("ConnectionStringHostListProvider does not support getHostRole")
}

func (c *ConnectionStringHostListProvider) IdentifyConnection(_ driver.Conn) (*host_info_util.HostInfo, error) {
	slog.Debug(error_util.GetMessage("ConnectionStringHostListProvider.unsupportedIdentifyConnection"))
	return nil, nil
}

func (c *ConnectionStringHostListProvider) GetClusterId() (clusterId string, err error) {
	return "<none>", nil
}

func (c *ConnectionStringHostListProvider) CreateHost(hostName string, hostRole host_info_util.HostRole, lag float64, cpu float64, lastUpdateTime time.Time) *host_info_util.HostInfo {
	builder := host_info_util.NewHostInfoBuilder()
	weight := int(math.Round(lag)*100 + math.Round(cpu))
	port := c.hostListProviderService.GetDialect().GetDefaultPort()
	if hostName == "" {
		hostName = c.initialHost
	}
	builder.SetHost(hostName).SetPort(port).SetRole(hostRole).SetAvailability(host_info_util.AVAILABLE).SetWeight(weight).SetLastUpdateTime(lastUpdateTime)
	hostInfo, _ := builder.Build()
	return hostInfo
}

func (c *ConnectionStringHostListProvider) StopMonitor() {
	// do nothing since this provider doesn't implement active monitoring
}
