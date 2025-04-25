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
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/property_util"
	"awssql/utils"
	"database/sql/driver"
	"log/slog"
	"math"
	"time"
)

type DsnHostListProvider struct {
	isSingleWriterConnectionString bool
	dsn                            string
	hostListProviderService        HostListProviderService
	isInitialized                  bool
	hostList                       []*host_info_util.HostInfo
	initialHost                    string
}

func NewDsnHostListProvider(props map[string]string, dsn string, hostListProviderService HostListProviderService) *DsnHostListProvider {
	isSingleWriterConnectionString := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.SINGLE_WRITER_DSN)
	initialHost := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.HOST)
	return &DsnHostListProvider{
		isSingleWriterConnectionString,
		dsn,
		hostListProviderService,
		false,
		[]*host_info_util.HostInfo{},
		initialHost,
	}
}

func (c *DsnHostListProvider) init() error {
	if c.isInitialized {
		return nil
	}

	hosts, _ := utils.GetHostsFromDsn(c.dsn, c.isSingleWriterConnectionString)
	c.hostList = append(c.hostList, hosts...)

	if len(c.hostList) == 0 {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("DsnHostListProvider.parsedListEmpty"))
	}

	c.hostListProviderService.SetInitialConnectionHostInfo(c.hostList[0])
	c.isInitialized = true
	return nil
}

func (c *DsnHostListProvider) IsStaticHostListProvider() bool {
	return true
}

func (c *DsnHostListProvider) Refresh(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	err := c.init()
	return c.hostList, err
}

func (c *DsnHostListProvider) ForceRefresh(conn driver.Conn) ([]*host_info_util.HostInfo, error) {
	err := c.init()
	return c.hostList, err
}

func (c *DsnHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	slog.Warn(error_util.GetMessage("DsnHostListProvider.unsupportedGetHostRole"))
	return host_info_util.UNKNOWN
}

func (c *DsnHostListProvider) IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error) {
	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DsnHostListProvider.unsupportedIdentifyConnection"))
}

func (c *DsnHostListProvider) GetClusterId() (clusterId string) {
	slog.Warn(error_util.GetMessage("DsnHostListProvider.unsupportedGetClusterId"))
	return
}

func (c *DsnHostListProvider) CreateHost(hostName string, hostRole host_info_util.HostRole, lag float64, cpu float64, lastUpdateTime time.Time) *host_info_util.HostInfo {
	builder := host_info_util.NewHostInfoBuilder()
	weight := int(math.Round(lag)*100 + math.Round(cpu))
	port := c.hostListProviderService.GetDialect().GetDefaultPort()
	builder.SetHost(c.initialHost).SetPort(port).SetRole(hostRole).SetAvailability(host_info_util.AVAILABLE).SetWeight(weight).SetLastUpdateTime(lastUpdateTime)
	hostInfo, _ := builder.Build()
	return hostInfo
}
