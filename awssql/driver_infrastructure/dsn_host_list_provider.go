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

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type DsnHostListProvider struct {
	isSingleWriterConnectionString bool
	props                          *utils.RWMap[string, string]
	hostListProviderService        HostListProviderService
	isInitialized                  bool
	hostList                       []*host_info_util.HostInfo
	initialHost                    string
}

func NewDsnHostListProvider(props *utils.RWMap[string, string], hostListProviderService HostListProviderService) *DsnHostListProvider {
	isSingleWriterConnectionString := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.SINGLE_WRITER_DSN)
	initialHost := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.HOST)
	return &DsnHostListProvider{
		isSingleWriterConnectionString,
		props,
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

	hosts, err := property_util.GetHostsFromProps(c.props, c.isSingleWriterConnectionString)
	if err != nil {
		return err
	}
	c.hostList = append(c.hostList, hosts...)

	if len(c.hostList) == 0 {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("DsnHostListProvider.parsedListEmpty", property_util.MaskProperties(c.props)))
	}

	c.hostListProviderService.SetInitialConnectionHostInfo(c.hostList[0])
	c.isInitialized = true
	return nil
}

func (c *DsnHostListProvider) IsStaticHostListProvider() bool {
	return true
}

func (c *DsnHostListProvider) Refresh() ([]*host_info_util.HostInfo, error) {
	err := c.init()
	return host_info_util.CopyHostList(c.hostList), err
}

func (c *DsnHostListProvider) ForceRefresh() ([]*host_info_util.HostInfo, error) {
	err := c.init()
	return host_info_util.CopyHostList(c.hostList), err
}

func (c *DsnHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	slog.Warn(error_util.GetMessage("DsnHostListProvider.unsupportedGetHostRole"))
	return host_info_util.UNKNOWN
}

func (c *DsnHostListProvider) IdentifyConnection(_ driver.Conn) (*host_info_util.HostInfo, error) {
	slog.Warn(error_util.GetMessage("DsnHostListProvider.unsupportedIdentifyConnection"))
	return nil, nil
}

func (c *DsnHostListProvider) GetClusterId() (clusterId string, err error) {
	return "<none>", nil
}

func (c *DsnHostListProvider) StopMonitor() {
	// do nothing since this provider doesn't implement active monitoring
}

func (c *DsnHostListProvider) ForceRefreshHostListWithTimeout(_ bool, _ int) ([]*host_info_util.HostInfo, error) {
	return c.ForceRefresh()
}
