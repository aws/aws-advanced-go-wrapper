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
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type AuroraInitialConnectionStrategyPluginFactory struct{}

func NewAuroraInitialConnectionStrategyPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return AuroraInitialConnectionStrategyPluginFactory{}
}

func (factory AuroraInitialConnectionStrategyPluginFactory) ClearCaches() {
}

func (factory AuroraInitialConnectionStrategyPluginFactory) GetInstance(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewAuroraInitialConnectionStrategyPlugin(servicesContainer.GetPluginService(), props)
}

func verifiedOpenedConnectionTypeFromString(value string) host_info_util.HostRole {
	if strings.ToLower(strings.TrimSpace(value)) == string(host_info_util.WRITER) {
		return host_info_util.WRITER
	} else if strings.ToLower(strings.TrimSpace(value)) == string(host_info_util.READER) {
		return host_info_util.READER
	} else {
		return host_info_util.UNKNOWN
	}
}

type AuroraInitialConnectionStrategyPlugin struct {
	BaseConnectionPlugin
	pluginService            driver_infrastructure.PluginService
	hostListProviderService  driver_infrastructure.HostListProviderService
	props                    *utils.RWMap[string, string]
	verifyOpenConnectionType host_info_util.HostRole
	retryDelayMs             int
	retryTimeoutMs           int
}

func NewAuroraInitialConnectionStrategyPlugin(
	pluginService driver_infrastructure.PluginService,
	props *utils.RWMap[string, string]) (*AuroraInitialConnectionStrategyPlugin, error) {
	retryDelayMs, err := property_util.GetPositiveIntProperty(props, property_util.INITIAL_CONNECTION_RETRY_INTERVAL_MS)
	if err != nil {
		return nil, err
	}
	retryTimeoutMs, err := property_util.GetPositiveIntProperty(props, property_util.INITIAL_CONNECTION_RETRY_TIMEOUT_MS)
	if err != nil {
		return nil, err
	}

	return &AuroraInitialConnectionStrategyPlugin{
		pluginService: pluginService,
		props:         props,
		verifyOpenConnectionType: verifiedOpenedConnectionTypeFromString(
			property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.VERIFY_INITIAL_CONNECTION_TYPE)),
		retryDelayMs:   retryDelayMs,
		retryTimeoutMs: retryTimeoutMs,
	}, nil
}

func (plugin *AuroraInitialConnectionStrategyPlugin) GetPluginCode() string {
	return driver_infrastructure.AURORA_INITIAL_CONNECTION_STRATEGY_PLUGIN_CODE
}

func (plugin *AuroraInitialConnectionStrategyPlugin) GetSubscribedMethods() []string {
	return []string{
		plugin_helpers.CONNECT_METHOD,
		plugin_helpers.INIT_HOST_PROVIDER_METHOD,
	}
}

func (plugin *AuroraInitialConnectionStrategyPlugin) InitHostProvider(
	props *utils.RWMap[string, string],
	hostListProviderService driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	plugin.hostListProviderService = hostListProviderService
	return initHostProviderFunc()
}

func (plugin *AuroraInitialConnectionStrategyPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	urlType := utils.IdentifyRdsUrlType(hostInfo.GetHost())
	if !urlType.IsRdsCluster {
		// It's not a cluster endpoint. Continue with a normal workflow.
		return connectFunc(props)
	}

	if urlType == utils.RDS_WRITER_CLUSTER ||
		isInitialConnection && plugin.verifyOpenConnectionType == host_info_util.WRITER {
		writerCandidateConn, err := plugin.getVerifiedWriterConnection(props, isInitialConnection, connectFunc)
		if err != nil {
			return nil, err
		}
		if writerCandidateConn == nil {
			// Can't get writer connection. Continue with a normal workflow.
			return connectFunc(props)
		}
		return writerCandidateConn, nil
	}

	if urlType == utils.RDS_READER_CLUSTER ||
		isInitialConnection && plugin.verifyOpenConnectionType == host_info_util.READER {
		readerCandidateConn, err := plugin.getVerifiedReaderConnection(urlType, hostInfo, props, isInitialConnection, connectFunc)
		if err != nil {
			return nil, err
		}
		if readerCandidateConn == nil {
			// Can't get reader connection. Continue with a normal workflow.
			return connectFunc(props)
		}
		return readerCandidateConn, nil
	}

	return connectFunc(props)
}

func (plugin *AuroraInitialConnectionStrategyPlugin) getVerifiedWriterConnection(
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	endTime := time.Now().Add(time.Millisecond * time.Duration(plugin.retryTimeoutMs))

	var writerCandidateConn driver.Conn
	var writerCandidate *host_info_util.HostInfo
	var err error
	for time.Now().Before(endTime) {
		writerCandidateConn = nil
		writerCandidate = host_info_util.GetWriter(plugin.pluginService.GetAllHosts())
		if writerCandidate == nil || utils.IsRdsClusterDns(writerCandidate.GetHost()) {
			// Writer is not found. It seems that topology is outdated.
			writerCandidateConn, err = connectFunc(props)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
					continue
				}
				return nil, err
			}
			err = plugin.pluginService.ForceRefreshHostList(writerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
					continue
				}
				return nil, err
			}

			writerCandidate, err = plugin.pluginService.IdentifyConnection(writerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
					continue
				}
				return nil, err
			}

			if writerCandidate == nil || writerCandidate.Role != host_info_util.WRITER {
				// Shouldn't be here. But let's try again.
				_ = writerCandidateConn.Close()
				plugin.delayMs()
				continue
			}

			if isInitialConnection {
				plugin.hostListProviderService.SetInitialConnectionHostInfo(writerCandidate)
			}
			return writerCandidateConn, nil
		}
		writerCandidateConn, err := plugin.pluginService.Connect(writerCandidate, props, plugin)
		if err != nil {
			if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
				continue
			}
			return nil, err
		}
		if plugin.pluginService.GetHostRole(writerCandidateConn) != host_info_util.WRITER {
			// If the new connection resolves to a reader instance, this means the topology is outdated.
			// Force refresh to update the topology.
			err = plugin.pluginService.ForceRefreshHostList(writerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
					continue
				}
				return nil, err
			}
			_ = writerCandidateConn.Close()
			plugin.delayMs()
			continue
		}
		if isInitialConnection {
			plugin.hostListProviderService.SetInitialConnectionHostInfo(writerCandidate)
		}
		return writerCandidateConn, nil
	}
	return nil, nil
}

func (plugin *AuroraInitialConnectionStrategyPlugin) getVerifiedReaderConnection(
	urlType utils.RdsUrlType,
	host *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	endTime := time.Now().Add(time.Millisecond * time.Duration(plugin.retryTimeoutMs))

	var readerCandidateConn driver.Conn
	var readerCandidateHost *host_info_util.HostInfo
	var err error
	var awsRegion = ""
	if urlType == utils.RDS_READER_CLUSTER {
		awsRegion = utils.GetRdsRegion(host.GetHost())
	}

	for time.Now().Before(endTime) {
		readerCandidateConn = nil
		readerCandidateHost, err = plugin.getReader(props, awsRegion)
		if err != nil {
			if plugin.handleErrorAndShouldRetry(readerCandidateConn, readerCandidateHost, err) {
				continue
			}
			return nil, err
		}

		if readerCandidateHost == nil || utils.IsRdsClusterDns(readerCandidateHost.GetHost()) {
			// Reader is not found. It seems that topology is outdated.
			readerCandidateConn, err = connectFunc(props)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(readerCandidateConn, readerCandidateHost, err) {
					continue
				}
				return nil, err
			}

			err = plugin.pluginService.ForceRefreshHostList(readerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(readerCandidateConn, readerCandidateHost, err) {
					continue
				}
				return nil, err
			}
			readerCandidateHost, err = plugin.pluginService.IdentifyConnection(readerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(readerCandidateConn, readerCandidateHost, err) {
					continue
				}
				return nil, err
			}

			if readerCandidateHost == nil {
				if readerCandidateConn != nil {
					_ = readerCandidateConn.Close()
				}
				plugin.delayMs()
				continue
			}

			if readerCandidateHost.Role != host_info_util.READER {
				if plugin.hasNoReaders() {
					// It seems that cluster has no readers. Simulate Aurora reader cluster endpoint logic
					// and return the current (writer) connection.
					if isInitialConnection {
						plugin.hostListProviderService.SetInitialConnectionHostInfo(readerCandidateHost)
					}
					return readerCandidateConn, nil
				}
				if readerCandidateConn != nil {
					_ = readerCandidateConn.Close()
				}
				plugin.delayMs()
				continue
			}

			if isInitialConnection {
				plugin.hostListProviderService.SetInitialConnectionHostInfo(readerCandidateHost)
			}
			return readerCandidateConn, nil
		}

		readerCandidateConn, err = plugin.pluginService.Connect(readerCandidateHost, props, plugin)
		if err != nil {
			if plugin.handleErrorAndShouldRetry(readerCandidateConn, readerCandidateHost, err) {
				continue
			}
			return nil, err
		}

		if plugin.pluginService.GetHostRole(readerCandidateConn) != host_info_util.READER {
			// If the new connection resolves to a writer instance, this means the topology is outdated.
			// Force refresh to update the topology.
			err = plugin.pluginService.ForceRefreshHostList(readerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(readerCandidateConn, readerCandidateHost, err) {
					continue
				}
				return nil, err
			}

			if plugin.hasNoReaders() {
				// It seems that cluster has no readers. Simulate Aurora reader cluster endpoint logic
				// and return the current (writer) connection.
				if isInitialConnection {
					plugin.hostListProviderService.SetInitialConnectionHostInfo(readerCandidateHost)
				}
				return readerCandidateConn, nil
			}
			_ = readerCandidateConn.Close()
			plugin.delayMs()
			continue
		}
		if isInitialConnection {
			plugin.hostListProviderService.SetInitialConnectionHostInfo(readerCandidateHost)
		}
		return readerCandidateConn, nil
	}
	return nil, nil
}

func (plugin *AuroraInitialConnectionStrategyPlugin) handleErrorAndShouldRetry(
	candidateConn driver.Conn,
	candidate *host_info_util.HostInfo,
	err error) bool {
	if err == nil {
		return false
	}
	if candidateConn != nil {
		_ = candidateConn.Close()
	}
	slog.Debug(error_util.GetMessage("AuroraInitialConnectionStrategyPlugin.errorGettingConnection", err))
	if plugin.pluginService.IsLoginError(err) {
		return false
	}
	if candidate != nil {
		plugin.pluginService.SetAvailability(candidate.GetAllAliases(), host_info_util.UNAVAILABLE)
	}
	plugin.delayMs()
	return true
}

func (plugin *AuroraInitialConnectionStrategyPlugin) getReader(props *utils.RWMap[string, string], awsRegion string) (*host_info_util.HostInfo, error) {
	strategy := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.READER_INITIAL_CONN_HOST_SELECTOR_STRATEGY)
	if plugin.pluginService.AcceptsStrategy(strategy) {
		var hostCandidates []*host_info_util.HostInfo
		if awsRegion != "" {
			hostCandidates = utils.FilterSlice(
				plugin.pluginService.GetHosts(),
				func(hostInfo *host_info_util.HostInfo) bool {
					return strings.EqualFold(awsRegion, utils.GetRdsRegion(hostInfo.GetHost()))
				})
		} else {
			hostCandidates = plugin.pluginService.GetHosts()
		}

		host, err := plugin.pluginService.GetHostInfoByStrategy(host_info_util.READER, strategy, hostCandidates)
		if err != nil {
			return nil, nil
		}
		return host, nil
	}
	return nil, error_util.NewUnsupportedStrategyError(error_util.GetMessage("AuroraInitialConnectionStrategyPlugin.unsupportedStrategy", strategy))
}

func (plugin *AuroraInitialConnectionStrategyPlugin) hasNoReaders() bool {
	if len(plugin.pluginService.GetAllHosts()) < 1 {
		// Topology inconclusive/corrupted.
		return false
	}
	for _, hostInfo := range plugin.pluginService.GetAllHosts() {
		if hostInfo.Role == host_info_util.WRITER {
			continue
		}
		// Found a reader node
		return false
	}
	// Went through all hosts and found no reader
	return true
}

func (plugin *AuroraInitialConnectionStrategyPlugin) delayMs() {
	time.Sleep(time.Millisecond * time.Duration(plugin.retryDelayMs))
}
