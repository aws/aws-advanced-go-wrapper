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

type DsnHostListProvider struct {
	isSingleWriterConnectionString bool
	initialUrl                     string
	dsnParser                      DsnParser
	hostListProviderService        HostListProviderService
	isInitialized                  bool
	hostList                       []HostInfo
}

func NewConnectionStringHostListProvider(props map[string]any, initialUrl string, hostListProviderService HostListProviderService, parser DsnParser) *DsnHostListProvider {
	var isSingleWriterConnectionString, ok = (SINGLE_WRITER_CONNECTION_STRING.Get(props)).(bool)
	if !ok {
		isSingleWriterConnectionString = SINGLE_WRITER_CONNECTION_STRING.defaultValue.(bool)
	}
	return &DsnHostListProvider{isSingleWriterConnectionString, initialUrl, parser, hostListProviderService, false, make([]HostInfo, 0)}
}

func (c DsnHostListProvider) init() {
	if c.isInitialized {
		return
	}

	c.hostList = append(c.hostList, c.dsnParser.GetHostsFromDsn(c.initialUrl, c.isSingleWriterConnectionString, c.hostListProviderService.GetHostInfoBuilder)...)

	if len(c.hostList) == 0 {
		panic(GetAwsWrapperMessage("ConnectionStringHostListProvider.parsedListEmpty"))
	}

	c.hostListProviderService.SetInitialConnectionHostInfo(c.hostList[0])
	c.isInitialized = true
}

func (c DsnHostListProvider) IsStaticHostListProvider() bool {
	return true
}

func (c DsnHostListProvider) Refresh(conn *driver.Conn) []HostInfo {
	c.init()
	return c.hostList
}

func (c DsnHostListProvider) ForceRefresh(conn *driver.Conn) []HostInfo {
	c.init()
	return c.hostList
}

func (c DsnHostListProvider) getHostRole(conn *driver.Conn) HostRole {
	panic(GetAwsWrapperMessage("ConnectionStringHostListProvider.unsupportedGetHostRole"))
}

func (c DsnHostListProvider) IdentifyConnection(conn *driver.Conn) HostInfo {
	panic(GetAwsWrapperMessage("ConnectionStringHostListProvider.unsupportedIdentifyConnection"))
}

func (c DsnHostListProvider) GetClusterId() string {
	panic(GetAwsWrapperMessage("ConnectionStringHostListProvider.unsupportedGetClusterId"))
}
