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
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// GdbReadWriteSplittingPluginFactory creates a ReadWriteSplittingPlugin
// configured with a GDB strategy for region-aware reader/writer routing.
type GdbReadWriteSplittingPluginFactory struct{}

func (factory GdbReadWriteSplittingPluginFactory) GetInstance(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
) (driver_infrastructure.ConnectionPlugin, error) {
	return NewReadWriteSplittingPlugin(
		servicesContainer, props,
		driver_infrastructure.GDB_READ_WRITE_SPLITTING_PLUGIN_CODE,
		NewGdbReadWriteSplittingStrategy(props)), nil
}

func (factory GdbReadWriteSplittingPluginFactory) ClearCaches() {
}

func NewGdbReadWriteSplittingPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return GdbReadWriteSplittingPluginFactory{}
}

// GdbReadWriteSplittingStrategy implements ReadWriteSplittingStrategy
// with region-aware filtering for Aurora Global Databases.
// It embeds DefaultReadWriteSplittingStrategy for the base topology logic.
type GdbReadWriteSplittingStrategy struct {
	DefaultReadWriteSplittingStrategy
	homeRegion           region_util.Region
	restrictWriterToHome bool
	restrictReaderToHome bool
	isInit               bool
	props                *utils.RWMap[string, string]
}

func NewGdbReadWriteSplittingStrategy(props *utils.RWMap[string, string]) *GdbReadWriteSplittingStrategy {
	return &GdbReadWriteSplittingStrategy{
		restrictWriterToHome: property_util.GetVerifiedWrapperPropertyValue[bool](
			props, property_util.GDB_RW_RESTRICT_WRITER_TO_HOME_REGION),
		restrictReaderToHome: property_util.GetVerifiedWrapperPropertyValue[bool](
			props, property_util.GDB_RW_RESTRICT_READER_TO_HOME_REGION),
		props: props,
	}
}

func (s *GdbReadWriteSplittingStrategy) OnConnect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
) error {
	if s.isInit {
		return nil
	}

	s.homeRegion = region_util.GetRegion(
		hostInfo.GetHost(), props, property_util.GDB_RW_HOME_REGION)

	if s.homeRegion == "" {
		return error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("GdbReadWriteSplittingPlugin.missingHomeRegion", hostInfo.GetHost()))
	}

	s.isInit = true

	slog.Debug(error_util.GetMessage(
		"GdbReadWriteSplittingPlugin.parameterValue", "gdbRwHomeRegion", string(s.homeRegion)))
	return nil
}

func (s *GdbReadWriteSplittingStrategy) GetWriterConnection(
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

	if s.restrictWriterToHome {
		hostRegion := region_util.GetRegionFromHost(writerHost.GetHost())
		if !strings.EqualFold(string(hostRegion), string(s.homeRegion)) {
			return nil, nil, error_util.NewGenericAwsWrapperError(
				error_util.GetMessage("GdbReadWriteSplittingPlugin.writerNotInHomeRegion",
					writerHost.GetHost(), string(s.homeRegion)))
		}
	}

	conn, err := servicesContainer.GetPluginService().Connect(writerHost, props, pluginToSkip)
	if err != nil {
		return nil, nil, err
	}
	return conn, writerHost, nil
}

func (s *GdbReadWriteSplittingStrategy) GetReaderConnection(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
	hosts []*host_info_util.HostInfo,
	readerSelectorStrategy string,
	pluginToSkip driver_infrastructure.ConnectionPlugin,
) (driver.Conn, *host_info_util.HostInfo, error) {
	filtered := hosts
	if s.restrictReaderToHome {
		filtered = nil
		for _, h := range hosts {
			hostRegion := region_util.GetRegionFromHost(h.GetHost())
			if strings.EqualFold(string(hostRegion), string(s.homeRegion)) {
				filtered = append(filtered, h)
			}
		}
		if len(filtered) == 0 {
			return nil, nil, error_util.NewGenericAwsWrapperError(
				error_util.GetMessage("GdbReadWriteSplittingPlugin.noReadersInHomeRegion", string(s.homeRegion)))
		}
	}

	// Delegate to the embedded default strategy for the retry/connect logic.
	return s.DefaultReadWriteSplittingStrategy.GetReaderConnection(
		servicesContainer, props, filtered, readerSelectorStrategy, pluginToSkip)
}
