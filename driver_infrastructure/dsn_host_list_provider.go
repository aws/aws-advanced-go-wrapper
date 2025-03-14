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
	"awssql/utils"
	"database/sql/driver"
)

type DsnHostListProvider struct {
	isSingleWriterConnectionString bool
	dsn                            string
	hostListProviderService        HostListProviderService
	isInitialized                  bool
	hostList                       []host_info_util.HostInfo
}

func NewDsnHostListProvider(props map[string]string, dsn string, hostListProviderService HostListProviderService) *DsnHostListProvider {
	isSingleWriterConnectionString := GetVerifiedWrapperPropertyValue[bool](props, SINGLE_WRITER_DSN)
	return &DsnHostListProvider{isSingleWriterConnectionString, dsn, hostListProviderService, false, []host_info_util.HostInfo{}}
}

func (c *DsnHostListProvider) init() {
	if c.isInitialized {
		return
	}

	hosts, _ := utils.GetHostsFromDsn(c.dsn, c.isSingleWriterConnectionString)
	c.hostList = append(c.hostList, hosts...)

	if len(c.hostList) == 0 {
		panic(error_util.GetMessage("DsnHostListProvider.parsedListEmpty"))
	}

	c.hostListProviderService.SetInitialConnectionHostInfo(c.hostList[0])
	c.isInitialized = true
}

func (c *DsnHostListProvider) IsStaticHostListProvider() bool {
	return true
}

func (c *DsnHostListProvider) Refresh(conn driver.Conn) []host_info_util.HostInfo {
	c.init()
	return c.hostList
}

func (c *DsnHostListProvider) ForceRefresh(conn driver.Conn) []host_info_util.HostInfo {
	c.init()
	return c.hostList
}

func (c *DsnHostListProvider) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	panic(error_util.GetMessage("DsnHostListProvider.unsupportedGetHostRole"))
}

func (c *DsnHostListProvider) IdentifyConnection(conn driver.Conn) host_info_util.HostInfo {
	panic(error_util.GetMessage("DsnHostListProvider.unsupportedIdentifyConnection"))
}

func (c *DsnHostListProvider) GetClusterId() string {
	panic(error_util.GetMessage("DsnHostListProvider.unsupportedGetClusterId"))
}
