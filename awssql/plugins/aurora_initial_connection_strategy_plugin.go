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

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
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
	return NewAuroraInitialConnectionStrategyPlugin(servicesContainer, props)
}

// verifyInitialConnectionTypeNone means explicitly declining verification even for endpoints that imply a role.
const verifyInitialConnectionTypeNone = "none"

// verifiedOpenedConnectionTypeFromString parses verifyInitialConnectionType.
// An unset or "none" value means no verification.
// An unrecognized value results in an error rather than a silent no-op.
func verifiedOpenedConnectionTypeFromString(value string) (host_info_util.HostRole, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(host_info_util.WRITER):
		return host_info_util.WRITER, nil
	case string(host_info_util.READER):
		return host_info_util.READER, nil
	case "", verifyInitialConnectionTypeNone:
		return host_info_util.UNKNOWN, nil
	default:
		return host_info_util.UNKNOWN, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("AuroraInitialConnectionStrategyPlugin.invalidPropertyValue",
				property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name,
				value,
				strings.Join([]string{
					string(host_info_util.WRITER),
					string(host_info_util.READER),
					verifyInitialConnectionTypeNone,
				}, ", ")))
	}
}

type AuroraInitialConnectionStrategyPlugin struct {
	BaseConnectionPlugin
	servicesContainer        driver_infrastructure.ServicesContainer
	hostListProviderService  driver_infrastructure.HostListProviderService
	props                    *utils.RWMap[string, string]
	verifyOpenConnectionType host_info_util.HostRole
	verificationDeclined     bool
	retryDelayMs             int
	retryTimeoutMs           int
}

func NewAuroraInitialConnectionStrategyPlugin(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string]) (*AuroraInitialConnectionStrategyPlugin, error) {
	retryDelayMs, err := property_util.GetPositiveIntProperty(props, property_util.INITIAL_CONNECTION_RETRY_INTERVAL_MS)
	if err != nil {
		return nil, err
	}
	retryTimeoutMs, err := property_util.GetPositiveIntProperty(props, property_util.INITIAL_CONNECTION_RETRY_TIMEOUT_MS)
	if err != nil {
		return nil, err
	}
	verifyInitialConnectionType := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.VERIFY_INITIAL_CONNECTION_TYPE)
	verifyOpenConnectionType, err := verifiedOpenedConnectionTypeFromString(verifyInitialConnectionType)
	if err != nil {
		return nil, err
	}

	return &AuroraInitialConnectionStrategyPlugin{
		servicesContainer:        servicesContainer,
		props:                    props,
		verifyOpenConnectionType: verifyOpenConnectionType,
		verificationDeclined: strings.EqualFold(
			strings.TrimSpace(verifyInitialConnectionType), verifyInitialConnectionTypeNone),
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

	switch plugin.roleToVerify(urlType, isInitialConnection) {
	case host_info_util.WRITER:
		if err := plugin.requireInstanceHostPattern(urlType, props); err != nil {
			return nil, err
		}
		return plugin.getVerifiedWriterConnection(props, isInitialConnection, connectFunc)
	case host_info_util.READER:
		if err := plugin.requireInstanceHostPattern(urlType, props); err != nil {
			return nil, err
		}
		return plugin.getVerifiedReaderConnection(urlType, hostInfo, props, isInitialConnection, connectFunc)
	default:
		if isInitialConnection && plugin.verifyOpenConnectionType != host_info_util.UNKNOWN {
			slog.Warn(error_util.GetMessage("AuroraInitialConnectionStrategyPlugin.roleVerificationUnsupportedForEndpoint",
				property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, hostInfo.GetHost()))
		}
		return connectFunc(props)
	}
}

// roleToVerify returns the role an initial connection must be verified against, or UNKNOWN when no
// verification applies.
func (plugin *AuroraInitialConnectionStrategyPlugin) roleToVerify(
	urlType utils.RdsUrlType,
	isInitialConnection bool) host_info_util.HostRole {
	if plugin.verificationDeclined {
		return host_info_util.UNKNOWN
	}

	switch urlType {
	case utils.RDS_WRITER_CLUSTER, utils.RDS_GLOBAL_WRITER_CLUSTER:
		return host_info_util.WRITER
	case utils.RDS_READER_CLUSTER:
		if isInitialConnection && plugin.verifyOpenConnectionType == host_info_util.WRITER {
			return host_info_util.WRITER
		}
		return host_info_util.READER
	case utils.RDS_CUSTOM_CLUSTER, utils.OTHER, utils.IP_ADDRESS:
		if isInitialConnection {
			return plugin.verifyOpenConnectionType
		}
		return host_info_util.UNKNOWN
	default:
		return host_info_util.UNKNOWN
	}
}

// requireInstanceHostPattern fails fast when a non-RDS endpoint has no clusterInstanceHostPattern.
func (plugin *AuroraInitialConnectionStrategyPlugin) requireInstanceHostPattern(
	urlType utils.RdsUrlType,
	props *utils.RWMap[string, string]) error {
	if urlType != utils.OTHER && urlType != utils.IP_ADDRESS {
		return nil
	}
	if property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.CLUSTER_INSTANCE_HOST_PATTERN) != "" {
		return nil
	}
	return error_util.NewGenericAwsWrapperError(
		error_util.GetMessage("AuroraInitialConnectionStrategyPlugin.instanceHostPatternRequired",
			property_util.CLUSTER_INSTANCE_HOST_PATTERN.Name,
			property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name))
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
		writerCandidate = host_info_util.GetWriter(plugin.servicesContainer.GetPluginService().GetAllHosts())
		if writerCandidate == nil || utils.IsRdsClusterDns(writerCandidate.GetHost()) {
			// Writer is not found. It seems that topology is outdated.
			writerCandidateConn, err = connectFunc(props)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
					continue
				}
				return nil, err
			}
			err = plugin.servicesContainer.GetPluginService().ForceRefreshHostList(writerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
					continue
				}
				return nil, err
			}

			writerCandidate, err = plugin.servicesContainer.GetPluginService().IdentifyConnection(writerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
					continue
				}
				return nil, err
			}

			if writerCandidate == nil || writerCandidate.Role != host_info_util.WRITER {
				// Shouldn't be here. But let's try again.
				if writerCandidateConn != nil {
					_ = writerCandidateConn.Close()
				}
				plugin.delayMs()
				continue
			}

			if isInitialConnection {
				plugin.hostListProviderService.SetInitialConnectionHostInfo(writerCandidate)
			}
			return writerCandidateConn, nil
		}
		writerCandidateConn, err = plugin.servicesContainer.GetPluginService().Connect(writerCandidate, props, plugin)
		if err != nil {
			if plugin.handleErrorAndShouldRetry(writerCandidateConn, writerCandidate, err) {
				continue
			}
			return nil, err
		}
		if plugin.servicesContainer.GetPluginService().GetHostRole(writerCandidateConn) != host_info_util.WRITER {
			// If the new connection resolves to a reader instance, this means the topology is outdated.
			// Force refresh to update the topology.
			err = plugin.servicesContainer.GetPluginService().ForceRefreshHostList(writerCandidateConn)
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
	return plugin.retryWindowExpired(host_info_util.WRITER, isInitialConnection, props, connectFunc)
}

// retryWindowExpired handles a verified-connection attempt that ran out of time. When the caller
// explicitly asked for a role via verifyInitialConnectionType, silently returning an unverified
// connection would hand back exactly what they asked to have checked, so we throw an error instead.
func (plugin *AuroraInitialConnectionStrategyPlugin) retryWindowExpired(
	role host_info_util.HostRole,
	isInitialConnection bool,
	props *utils.RWMap[string, string],
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if isInitialConnection && plugin.verifyOpenConnectionType != host_info_util.UNKNOWN {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("AuroraInitialConnectionStrategyPlugin.timeout",
				plugin.retryTimeoutMs, property_util.VERIFY_INITIAL_CONNECTION_TYPE.Name, role))
	}
	// Can't get a verified connection. Continue with a normal workflow.
	return connectFunc(props)
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

			err = plugin.servicesContainer.GetPluginService().ForceRefreshHostList(readerCandidateConn)
			if err != nil {
				if plugin.handleErrorAndShouldRetry(readerCandidateConn, readerCandidateHost, err) {
					continue
				}
				return nil, err
			}
			readerCandidateHost, err = plugin.servicesContainer.GetPluginService().IdentifyConnection(readerCandidateConn)
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

		readerCandidateConn, err = plugin.servicesContainer.GetPluginService().Connect(readerCandidateHost, props, plugin)
		if err != nil {
			if plugin.handleErrorAndShouldRetry(readerCandidateConn, readerCandidateHost, err) {
				continue
			}
			return nil, err
		}

		if plugin.servicesContainer.GetPluginService().GetHostRole(readerCandidateConn) != host_info_util.READER {
			// If the new connection resolves to a writer instance, this means the topology is outdated.
			// Force refresh to update the topology.
			err = plugin.servicesContainer.GetPluginService().ForceRefreshHostList(readerCandidateConn)
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
	return plugin.retryWindowExpired(host_info_util.READER, isInitialConnection, props, connectFunc)
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
	if plugin.servicesContainer.GetPluginService().IsLoginError(err) {
		return false
	}
	if candidate != nil {
		plugin.servicesContainer.GetPluginService().SetAvailability(candidate.GetAllAliases(), host_info_util.UNAVAILABLE)
	}
	plugin.delayMs()
	return true
}

func (plugin *AuroraInitialConnectionStrategyPlugin) getReader(props *utils.RWMap[string, string], awsRegion string) (*host_info_util.HostInfo, error) {
	strategy := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.READER_INITIAL_CONN_HOST_SELECTOR_STRATEGY)
	if plugin.servicesContainer.GetPluginService().AcceptsStrategy(strategy) {
		var hostCandidates []*host_info_util.HostInfo
		if awsRegion != "" {
			hostCandidates = utils.FilterSlice(
				plugin.servicesContainer.GetPluginService().GetHosts(),
				func(hostInfo *host_info_util.HostInfo) bool {
					return strings.EqualFold(awsRegion, utils.GetRdsRegion(hostInfo.GetHost()))
				})
		} else {
			hostCandidates = plugin.servicesContainer.GetPluginService().GetHosts()
		}

		host, err := plugin.servicesContainer.GetPluginService().GetHostInfoByStrategy(host_info_util.READER, strategy, hostCandidates)
		if err != nil {
			slog.Debug(error_util.GetMessage("AuroraInitialConnectionStrategyPlugin.errorGettingConnection", err))
			return nil, nil
		}
		return host, nil
	}
	return nil, error_util.NewUnsupportedStrategyError(error_util.GetMessage("AuroraInitialConnectionStrategyPlugin.unsupportedStrategy", strategy))
}

func (plugin *AuroraInitialConnectionStrategyPlugin) hasNoReaders() bool {
	if len(plugin.servicesContainer.GetPluginService().GetAllHosts()) < 1 {
		// Topology inconclusive/corrupted.
		return false
	}
	for _, hostInfo := range plugin.servicesContainer.GetPluginService().GetAllHosts() {
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
