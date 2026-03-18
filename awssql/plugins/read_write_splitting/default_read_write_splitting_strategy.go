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

package read_write_splitting

import (
	"database/sql/driver"
	"log/slog"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// DefaultReadWriteSplittingStrategy provides the base topology-aware
// read/write splitting behavior with no additional filtering or validation.
type DefaultReadWriteSplittingStrategy struct{}

func (s *DefaultReadWriteSplittingStrategy) OnConnect(
	_ *host_info_util.HostInfo,
	_ *utils.RWMap[string, string],
) error {
	return nil
}

func (s *DefaultReadWriteSplittingStrategy) GetWriterConnection(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
	hosts []*host_info_util.HostInfo,
	pluginToSkip driver_infrastructure.ConnectionPlugin,
) (driver.Conn, *host_info_util.HostInfo, error) {
	writerHost := host_info_util.GetWriter(hosts)
	if writerHost == nil {
		return nil, nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ReadWriteSplittingPlugin.noWriterFound"))
	}
	conn, err := servicesContainer.GetPluginService().Connect(writerHost, props, pluginToSkip)
	if err != nil {
		return nil, nil, err
	}
	return conn, writerHost, nil
}

func (s *DefaultReadWriteSplittingStrategy) GetReaderConnection(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
	hosts []*host_info_util.HostInfo,
	readerSelectorStrategy string,
	pluginToSkip driver_infrastructure.ConnectionPlugin,
) (driver.Conn, *host_info_util.HostInfo, error) {
	connAttempts := len(hosts) * 2
	for range connAttempts {
		hostInfo, err := servicesContainer.GetPluginService().GetHostInfoByStrategy(
			host_info_util.READER, readerSelectorStrategy, hosts)
		if err != nil {
			if hostInfo != nil {
				slog.Warn(error_util.GetMessage("ReadWriteSplittingPlugin.failedToConnectToReader", hostInfo.GetUrl()))
			} else {
				slog.Warn(error_util.GetMessage("ReadWriteSplittingPlugin.failedToConnectToReader", "unknown host"))
			}
			continue
		}

		conn, err := servicesContainer.GetPluginService().Connect(hostInfo, props, pluginToSkip)
		if err == nil {
			return conn, hostInfo, nil
		}
	}

	return nil, nil, error_util.NewGenericAwsWrapperError(
		error_util.GetMessage("ReadWriteSplittingPlugin.noReadersAvailable"))
}
