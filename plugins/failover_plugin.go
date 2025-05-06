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
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"awssql/property_util"
	"awssql/utils"
	"database/sql/driver"
	"errors"
	"log/slog"
	"strings"
	"time"
)

type FailoverMode string

const (
	MODE_STRICT_WRITER    FailoverMode = "strict-writer"
	MODE_STRICT_READER    FailoverMode = "strict-reader"
	MODE_READER_OR_WRITER FailoverMode = "reader-or-writer"
	MODE_UNKNOWN          FailoverMode = "unknown"
)

func failoverModeFromValue(mode string) FailoverMode {
	switch mode {
	case "strict-writer":
		return MODE_STRICT_WRITER
	case "strict-reader":
		return MODE_STRICT_READER
	case "reader-or-writer":
		return MODE_READER_OR_WRITER
	default:
		return MODE_UNKNOWN
	}
}

type ReaderFailoverResult struct {
	Conn     driver.Conn
	HostInfo *host_info_util.HostInfo
}

type FailoverPluginFactory struct{}

func (f FailoverPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService, props map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	return NewFailoverPlugin(pluginService, props), nil
}

func NewFailoverPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return FailoverPluginFactory{}
}

type FailoverPlugin struct {
	pluginService                             driver_infrastructure.PluginService
	hostListProviderService                   driver_infrastructure.HostListProviderService
	props                                     map[string]string
	failoverTimeoutMsSetting                  int
	failoverReaderHostSelectorStrategySetting string
	FailoverMode                              FailoverMode
	isInTransaction                           bool
	rdsUrlType                                utils.RdsUrlType
	lastErrorDealtWith                        error
	staleDnsHelper                            *StaleDnsHelper
	BaseConnectionPlugin
}

func NewFailoverPlugin(pluginService driver_infrastructure.PluginService, props map[string]string) *FailoverPlugin {
	failoverTimeoutMsSetting := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.FAILOVER_TIMEOUT_MS)
	failoverReaderHostSelectorStrategySetting := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.FAILOVER_READER_HOST_SELECTOR_STRATEGY)
	return &FailoverPlugin{
		pluginService:            pluginService,
		props:                    props,
		failoverTimeoutMsSetting: failoverTimeoutMsSetting,
		failoverReaderHostSelectorStrategySetting: failoverReaderHostSelectorStrategySetting,
		staleDnsHelper: &StaleDnsHelper{pluginService: pluginService},
	}
}

func (p *FailoverPlugin) GetSubscribedMethods() []string {
	return append([]string{
		plugin_helpers.CONNECT_METHOD,
		plugin_helpers.INIT_HOST_PROVIDER_METHOD,
	}, driver_infrastructure.NETWORK_BOUND_METHODS...)
}

func (p *FailoverPlugin) InitHostProvider(
	initialUrl string,
	props map[string]string,
	hostListProviderService driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	p.hostListProviderService = hostListProviderService
	return initHostProviderFunc()
}

func (p *FailoverPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	p.InitFailoverMode()

	var conn driver.Conn

	if !property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.ENABLE_CONNECT_FAILOVER) {
		return p.staleDnsHelper.GetVerifiedConnection(hostInfo.Host, isInitialConnection, p.hostListProviderService, props, connectFunc)
	}

	var hostInfoWithAvailability *host_info_util.HostInfo
	hosts := utils.FilterSlice(p.pluginService.GetHosts(), func(item *host_info_util.HostInfo) bool {
		return item.GetHostAndPort() == hostInfo.GetHostAndPort()
	})
	if len(hosts) != 0 {
		hostInfoWithAvailability = hosts[0]
	}

	if hostInfoWithAvailability.IsNil() || hostInfoWithAvailability.Availability != host_info_util.UNAVAILABLE {
		var err error
		conn, err = p.staleDnsHelper.GetVerifiedConnection(hostInfo.Host, isInitialConnection, p.hostListProviderService, props, connectFunc)
		if err != nil {
			if !p.shouldErrorTriggerConnectionSwitch(err) {
				return nil, err
			}

			p.pluginService.SetAvailability(hostInfo.AllAliases, host_info_util.UNAVAILABLE)

			err = p.Failover()
			if errors.Is(err, error_util.FailoverSuccessError) {
				conn = p.pluginService.GetCurrentConnection()
			}
		}
	} else {
		refreshErr := p.pluginService.RefreshHostList(conn)
		if refreshErr != nil {
			return nil, refreshErr
		}
		err := p.Failover()
		if errors.Is(err, error_util.FailoverSuccessError) {
			conn = p.pluginService.GetCurrentConnection()
		}
	}

	if conn == nil {
		// This should be unreachable, the above logic will either get a connection successfully or return an error.
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("Failover.unableToConnect"))
	}

	if isInitialConnection {
		refreshErr := p.pluginService.RefreshHostList(conn)
		if refreshErr != nil {
			return nil, refreshErr
		}
	}

	return conn, nil
}

func (p *FailoverPlugin) InitFailoverMode() {
	if p.rdsUrlType == utils.OTHER {
		p.FailoverMode = failoverModeFromValue(strings.ToLower(property_util.GetVerifiedWrapperPropertyValue[string](p.props, property_util.FAILOVER_MODE)))
		initialHostInfo := p.pluginService.GetInitialConnectionHostInfo()
		p.rdsUrlType = utils.IdentifyRdsUrlType(initialHostInfo.Host)

		if p.FailoverMode == MODE_UNKNOWN {
			if p.rdsUrlType == utils.RDS_READER_CLUSTER {
				p.FailoverMode = MODE_READER_OR_WRITER
			} else {
				p.FailoverMode = MODE_STRICT_WRITER
			}
		}

		slog.Debug(error_util.GetMessage("Failover.parameterValue", "failoverMode", p.FailoverMode))
	}
}

func (p *FailoverPlugin) Execute(
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	if p.canDirectExecute(methodName) {
		return executeFunc()
	}

	wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr = executeFunc()
	var err error
	if wrappedErr != nil {
		err = p.DealWithError(wrappedErr)
	}

	if err != nil {
		return nil, nil, false, err
	}

	return wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr
}

func (p *FailoverPlugin) DealWithError(err error) error {
	if err != nil {
		slog.Debug(error_util.GetMessage("Failover.detectedError", err.Error()))
		if !errors.Is(err, p.lastErrorDealtWith) && p.shouldErrorTriggerConnectionSwitch(err) {
			p.InvalidateCurrentConnection()
			currentHost, e := p.pluginService.GetCurrentHostInfo()
			if e != nil {
				return e
			}
			p.pluginService.SetAvailability(currentHost.Aliases, host_info_util.UNAVAILABLE)
			e = p.Failover()
			if e != nil {
				return e
			}
			p.lastErrorDealtWith = err
		}
	}
	return err
}

func (p *FailoverPlugin) isFailoverEnabled() bool {
	return p.rdsUrlType != utils.RDS_PROXY && len(p.pluginService.GetHosts()) != 0
}

func (p *FailoverPlugin) canDirectExecute(methodName string) bool {
	return methodName == driver_infrastructure.CONN_CLOSE
}

func (p *FailoverPlugin) Failover() error {
	if p.FailoverMode == MODE_STRICT_WRITER {
		return p.FailoverWriter()
	} else {
		return p.FailoverReader()
	}
}

func (p *FailoverPlugin) returnFailoverSuccessError() error {
	if p.isInTransaction || p.pluginService.IsInTransaction() {
		p.pluginService.SetInTransaction(false)

		// "Transaction resolution unknown. Please re-configure session state if required and try restarting transaction."
		message := error_util.GetMessage("Failover.transactionResolutionUnknownError")
		slog.Info(message)
		return error_util.TransactionResolutionUnknownError
	} else {
		// "The active SQL connection has changed due to a connection failure. Please re-configure session state if required."
		slog.Warn(error_util.GetMessage("Failover.connectionChangedError"))
		return error_util.FailoverSuccessError
	}
}

func (p *FailoverPlugin) FailoverWriter() error {
	failoverStartTime := time.Now()

	defer func() {
		slog.Info(error_util.GetMessage("Failover.writerFailoverElapsed", time.Since(failoverStartTime)))
	}()

	slog.Info(error_util.GetMessage("Failover.startWriterFailover"))
	// It's expected that this method synchronously returns when topology is stabilized,
	// i.e. when cluster control plane has already chosen a new writer.
	forceRefreshOk, _ := p.pluginService.ForceRefreshHostListWithTimeout(true, p.failoverTimeoutMsSetting)
	if !forceRefreshOk {
		slog.Error(error_util.GetMessage("Failover.unableToRefreshHostList"))
		return error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToRefreshHostList"))
	}

	updatedHosts := p.pluginService.GetHosts()

	writerCandidate := host_info_util.GetWriter(updatedHosts)
	if writerCandidate.IsNil() {
		message := utils.LogTopology(updatedHosts, error_util.GetMessage("Failover.noWriterHost"))
		slog.Error(message)
		return error_util.NewFailoverFailedError(message)
	}

	writerCandidateConn, err := p.pluginService.Connect(writerCandidate, p.props)
	if err != nil {
		slog.Error(error_util.GetMessage("Failover.errorConnectingToWriter", err.Error()))
		return error_util.NewFailoverFailedError(err.Error())
	}

	role := p.pluginService.GetHostRole(writerCandidateConn)
	if role != host_info_util.WRITER {
		_ = writerCandidateConn.Close()

		message := error_util.GetMessage("Failover.unexpectedReaderRole", writerCandidate.Host, role)
		slog.Error(message)
		return error_util.NewFailoverFailedError(message)
	}

	err = p.pluginService.SetCurrentConnection(writerCandidateConn, writerCandidate, nil)
	if err != nil {
		return err
	}

	currentHostInfo, err := p.pluginService.GetCurrentHostInfo()
	if currentHostInfo.IsNil() || err != nil {
		return err
	}
	slog.Info(error_util.GetMessage("Failover.establishedConnection", currentHostInfo.String()))
	return p.returnFailoverSuccessError()
}

func (p *FailoverPlugin) FailoverReader() error {
	failoverStartTime := time.Now()
	failoverEndTime := failoverStartTime.Add(time.Duration(p.failoverTimeoutMsSetting) * time.Millisecond)

	defer func() {
		slog.Info(error_util.GetMessage("Failover.readerFailoverElapsed", time.Since(failoverStartTime)))
	}()

	slog.Info(error_util.GetMessage("Failover.startReaderFailover"))
	// When we pass a timeout of 0, we inform the plugin service that it should update its topology without waiting
	// for it to get updated, since we do not need updated topology to establish a reader connection.
	forceRefreshOk, err := p.pluginService.ForceRefreshHostListWithTimeout(false, 0)
	if err != nil {
		return err
	}
	if !forceRefreshOk {
		slog.Error(error_util.GetMessage("Failover.failoverReaderUnableToRefreshHostList"))
		return error_util.NewFailoverFailedError(error_util.GetMessage("Failover.failoverReaderUnableToRefreshHostList"))
	}

	result, getReaderErr := p.getReaderFailoverConnection(failoverEndTime)
	if getReaderErr != nil {
		return p.returnReaderFailoverErr()
	}
	setConnErr := p.pluginService.SetCurrentConnection(result.Conn, result.HostInfo, nil)
	if setConnErr != nil {
		return p.returnReaderFailoverErr()
	}

	slog.Info(error_util.GetMessage("Failover.establishedConnection", result.HostInfo.String()))
	return p.returnFailoverSuccessError()
}

func (p *FailoverPlugin) returnReaderFailoverErr() error {
	slog.Error(error_util.GetMessage("Failover.unableToConnectToReader"))
	return error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToConnectToReader"))
}

func (p *FailoverPlugin) getReaderFailoverConnection(endTime time.Time) (ReaderFailoverResult, error) {
	// The roles in this list might not be accurate, depending on whether the new topology has become available yet.
	hosts := p.pluginService.GetHosts()
	readerCandidates := utils.FilterSlice(hosts, func(hostInfo *host_info_util.HostInfo) bool {
		return hostInfo.Role == host_info_util.READER
	})
	originalWriter := host_info_util.GetWriter(hosts)
	isOriginalWriterStillWriter := false

	// First, try all original readers
	for ok := true; ok; ok = time.Now().Before(endTime) {
		remainingReaders := readerCandidates

		for len(remainingReaders) > 0 && time.Now().Before(endTime) {
			readerCandidate, err := p.pluginService.GetHostInfoByStrategy(host_info_util.READER, p.failoverReaderHostSelectorStrategySetting, remainingReaders)
			if err != nil {
				slog.Debug(utils.LogTopology(remainingReaders, error_util.GetMessage("Failover.errorSelectingReaderHost", err)))
				break
			}

			if readerCandidate.IsNil() {
				slog.Debug(utils.LogTopology(remainingReaders, error_util.GetMessage("Failover.readerCandidateNil")))
				break
			}

			candidateConn, err := p.pluginService.Connect(readerCandidate, p.props)
			if candidateConn == nil || err != nil {
				remainingReaders = utils.RemoveFromSlice[*host_info_util.HostInfo](remainingReaders,
					readerCandidate,
					func(hostInfo1 *host_info_util.HostInfo, hostInfo2 *host_info_util.HostInfo) bool {
						return hostInfo1.Equals(hostInfo2)
					})
			} else {
				// Since the roles in the host list might not be accurate, we execute a query to check the instance's role.
				role := p.pluginService.GetHostRole(candidateConn)
				if role == host_info_util.READER || p.FailoverMode != MODE_STRICT_READER {
					updatedHostInfo := readerCandidate.MakeCopyWithRole(role)
					return ReaderFailoverResult{candidateConn, updatedHostInfo}, nil
				}

				// The role is WRITER or UNKNOWN, and we are in STRICT_READER mode, so the connection is not valid.
				remainingReaders = utils.RemoveFromSlice[*host_info_util.HostInfo](remainingReaders,
					readerCandidate,
					func(hostInfo1 *host_info_util.HostInfo, hostInfo2 *host_info_util.HostInfo) bool {
						return hostInfo1.Equals(hostInfo2)
					})
				_ = candidateConn.Close()

				if role == host_info_util.WRITER {
					// The reader candidate is actually a writer, which is not valid when failoverMode is STRICT_READER.
					// We will remove it from the list of reader candidates to avoid retrying it in future iterations.
					isOriginalWriterStillWriter = false
					readerCandidates = utils.RemoveFromSlice[*host_info_util.HostInfo](readerCandidates,
						readerCandidate,
						func(hostInfo1 *host_info_util.HostInfo, hostInfo2 *host_info_util.HostInfo) bool {
							return hostInfo1.Equals(hostInfo2)
						})
				} else {
					slog.Debug(error_util.GetMessage("Failover.strictReaderUnknownHostRole", originalWriter))
				}
			}
		}

		// We were not able to connect to any of the original readers. We will try connecting to the original writer,
		// which may have been demoted to a reader.
		if originalWriter.IsNil() || time.Now().After(endTime) {
			// No writer was found in the original topology, or we have timed out.
			continue
		}

		if p.FailoverMode == MODE_STRICT_READER && isOriginalWriterStillWriter {
			// The original writer has been verified, so it is not valid when in STRICT_READER mode.
			continue
		}

		// Try the original writer, which may have been demoted to a reader.
		candidateConn, err := p.pluginService.Connect(originalWriter, p.props)
		if candidateConn == nil || err != nil {
			slog.Debug(error_util.GetMessage("Failover.failedReaderConnection", originalWriter.Host))
		} else {
			// Since the roles in the host list might not be accurate, we execute a query to check the instance's role.
			role := p.pluginService.GetHostRole(candidateConn)
			if role == host_info_util.READER || p.FailoverMode != MODE_STRICT_READER {
				updatedHostInfo := originalWriter.MakeCopyWithRole(role)
				return ReaderFailoverResult{candidateConn, updatedHostInfo}, nil
			}

			// The role is WRITER or UNKNOWN, and we are in STRICT_READER mode, so the connection is not valid.
			_ = candidateConn.Close()

			if role == host_info_util.WRITER {
				isOriginalWriterStillWriter = true
			} else {
				slog.Debug(error_util.GetMessage("Failover.strictReaderUnknownHostRole", originalWriter))
			}
		}

		// All hosts failed. Keep trying until we hit the timeout.
	}

	return ReaderFailoverResult{}, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.failoverReaderTimeout"))
}

func (p *FailoverPlugin) InvalidateCurrentConnection() {
	conn := p.pluginService.GetCurrentConnection()
	if conn == nil {
		return
	}

	if p.pluginService.IsInTransaction() {
		p.isInTransaction = p.pluginService.IsInTransaction()
		utils.Rollback(conn, p.pluginService.GetCurrentTx())
		return
	}

	if !utils.IsConnectionLost(conn) {
		_ = conn.Close()
	}
}

func (p *FailoverPlugin) shouldErrorTriggerConnectionSwitch(err error) bool {
	if !p.isFailoverEnabled() {
		slog.Debug(error_util.GetMessage("Failover.failoverDisabled"))
		return false
	}

	return p.pluginService.IsNetworkError(err)
}
