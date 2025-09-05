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
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type ReadWriteSplittingPluginFactory struct{}

func (factory ReadWriteSplittingPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService,
	props map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	return NewReadWriteSplittingPlugin(pluginService, props), nil
}

func (factory ReadWriteSplittingPluginFactory) ClearCaches() {
}

func NewReadWriteSplittingPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return ReadWriteSplittingPluginFactory{}
}

type ReadWriteSplittingPlugin struct {
	plugins.BaseConnectionPlugin
	pluginService           driver_infrastructure.PluginService
	props                   map[string]string
	readerSelectorStrategy  string
	hostListProviderService driver_infrastructure.HostListProviderService
	inReadWriteSplit        bool
	writerConnection        driver.Conn
	readerConnection        driver.Conn
	writerHostInfo          *host_info_util.HostInfo
	readerHostInfo          *host_info_util.HostInfo
}

func NewReadWriteSplittingPlugin(pluginService driver_infrastructure.PluginService,
	props map[string]string) *ReadWriteSplittingPlugin {
	return &ReadWriteSplittingPlugin{
		pluginService: pluginService,
		props:         props,
		readerSelectorStrategy: property_util.GetVerifiedWrapperPropertyValue[string](props,
			property_util.READER_HOST_SELECTOR_STRATEGY),
	}
}

func (r *ReadWriteSplittingPlugin) GetPluginCode() string {
	return driver_infrastructure.READ_WRITE_SPLITTING_PLUGIN_CODE
}

func (r *ReadWriteSplittingPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD,
		plugin_helpers.INIT_HOST_PROVIDER_METHOD,
		plugin_helpers.NOTIFY_CONNECTION_CHANGED_METHOD,
		utils.CONN_QUERY_CONTEXT,
		utils.CONN_EXEC_CONTEXT,
	}
}

func (r *ReadWriteSplittingPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if !r.pluginService.AcceptsStrategy(r.readerSelectorStrategy) {
		msg := error_util.GetMessage("ReadWriteSplittingPlugin.unsupportedHostSelectorStrategy",
			r.readerSelectorStrategy, property_util.READER_HOST_SELECTOR_STRATEGY.Name)
		return nil, error_util.NewGenericAwsWrapperError(msg)
	}

	result, err := connectFunc(props)

	if err != nil {
		return nil, err
	}

	if !isInitialConnection || r.hostListProviderService.IsStaticHostListProvider() {
		return result, err
	}

	currentRole := r.pluginService.GetHostRole(result)
	if currentRole == host_info_util.UNKNOWN {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ReadWriteSplittingPlugin.errorVerifyingInitialHostRole"))
	}

	currentHost := r.pluginService.GetInitialConnectionHostInfo()

	if currentHost != nil {
		if currentRole == currentHost.Role {
			return result, err
		}
		updatedHostInfo := currentHost.MakeCopyWithRole(currentRole)
		r.hostListProviderService.SetInitialConnectionHostInfo(updatedHostInfo)
	}

	return result, err
}

func (r *ReadWriteSplittingPlugin) InitHostProvider(
	props map[string]string,
	hostListProviderService driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	r.hostListProviderService = hostListProviderService
	return initHostProviderFunc()
}

func (r *ReadWriteSplittingPlugin) NotifyConnectionChanged(
	changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	err := r.updateInternalConnectionInfo()
	if err != nil {
		slog.Warn(error_util.GetMessage("ReadWriteSplittingPlugin.updateInternalConnectionInfoFailed",
			err.Error()))
	}

	if r.inReadWriteSplit {
		return driver_infrastructure.PRESERVE
	}
	return driver_infrastructure.NO_OPINION
}

func (r *ReadWriteSplittingPlugin) updateInternalConnectionInfo() error {
	currentConn := r.pluginService.GetCurrentConnection()
	currentHost, err := r.pluginService.GetCurrentHostInfo()

	if err != nil {
		return err
	}

	if currentConn == nil || currentHost == nil {
		return nil
	}

	if r.isWriter(currentHost) {
		r.setWriterConnection(currentConn, currentHost)
	} else {
		r.setReaderConnection(currentConn, currentHost)
	}

	return nil
}

func (r *ReadWriteSplittingPlugin) Execute(
	connInvokedOn driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	query := utils.GetQueryFromSqlOrMethodArgs("", methodArgs)
	readOnly, found := utils.DoesSetReadOnly(query,
		r.pluginService.GetDialect().DoesStatementSetReadOnly)

	if found {
		err := r.switchConnectionIfRequired(readOnly)
		if err != nil {
			r.closeIdleConnections()
			return nil, nil, false, err
		}
	}

	wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr = executeFunc()

	awsWrapperError, ok := wrappedErr.(*error_util.AwsWrapperError)

	if ok && awsWrapperError.IsFailoverErrorType() {
		slog.Debug(error_util.GetMessage("ReadWriteSplittingPlugin.failoverErrorWhileExecutingCommand",
			awsWrapperError.Error()))
		r.closeIdleConnections()
	} else if awsWrapperError != nil {
		slog.Debug(error_util.GetMessage("ReadWriteSplittingPlugin.errorWhileExecutingCommand", wrappedErr))
	}

	return
}

func (r *ReadWriteSplittingPlugin) ReleaseResources() {
	r.closeIdleConnections()
}

func (r *ReadWriteSplittingPlugin) switchConnectionIfRequired(readOnly bool) error {
	currentConn := r.pluginService.GetCurrentConnection()
	if currentConn != nil && r.pluginService.GetTargetDriverDialect().IsClosed(currentConn) {
		return error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ReadWriteSplittingPlugin.setReadOnlyOnClosedConnection"))
	}

	if r.isConnectionUsable(currentConn) {
		err := r.pluginService.RefreshHostList(currentConn)
		if err != nil {
			// ignore
			slog.Debug(error_util.GetMessage("ReadWriteSplittingPlugin.couldNotRefreshHostlist"))
		}
	}

	hosts := r.pluginService.GetHosts()

	if len(hosts) == 0 {
		return error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ReadWriteSplittingPlugin.emptyHostList"))
	}

	currentHost, err := r.pluginService.GetCurrentHostInfo()
	if err != nil {
		return err
	}

	if readOnly {
		if !r.pluginService.IsInTransaction() && !r.isReader(currentHost) {
			err := r.switchToReaderConnection(hosts)
			if err != nil {
				if !r.isConnectionUsable(currentConn) {
					return error_util.NewGenericAwsWrapperError(
						error_util.GetMessage("ReadWriteSplittingPlugin.errorSwitchingToReader", err.Error()))
				}
				// Failed to switch to a reader, the current writer will be used as a fallback
				slog.Info(error_util.GetMessage("ReadWriteSplittingPlugin.fallbackToWriter",
					err.Error(), currentHost.GetUrl()))
			}
			return nil
		}
	}

	// Not readOnly
	if !r.isWriter(currentHost) && r.pluginService.IsInTransaction() {
		return error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ReadWriteSplittingPlugin.setReadOnlyFalseInTransaction"))
	}

	if !r.isWriter(currentHost) {
		err := r.switchToWriterConnection(hosts)
		if err != nil {
			return error_util.NewGenericAwsWrapperError(
				error_util.GetMessage("ReadWriteSplittingPlugin.errorSwitchingToWriter", err.Error()))
		}
	}
	return nil
}

func (r *ReadWriteSplittingPlugin) switchToReaderConnection(hosts []*host_info_util.HostInfo) error {
	currentConn := r.pluginService.GetCurrentConnection()
	currentHost, err := r.pluginService.GetCurrentHostInfo()
	if err != nil {
		return err
	}

	if r.isReader(currentHost) && r.isConnectionUsable(currentConn) {
		return nil
	}

	if r.readerHostInfo != nil && !host_info_util.IsHostInList(r.readerHostInfo, hosts) {
		// The old reader cannot be used anymore because it is no longer in the list of allowed hosts
		r.closeConnectionIfIdle(r.readerConnection)
	}

	r.inReadWriteSplit = true
	if !r.isConnectionUsable(r.readerConnection) {
		return r.initializeReaderConnection(hosts)
	}

	if err := r.switchCurrentConnectionTo(r.readerConnection, r.readerHostInfo); err != nil {
		slog.Warn(error_util.GetMessage("ReadWriteSplittingPlugin.errorSwitchingToCachedReader", r.readerHostInfo.GetUrl()))
		r.readerConnection.Close()
		r.readerConnection = nil
		r.readerHostInfo = nil
		return r.initializeReaderConnection(hosts)
	}
	slog.Info(error_util.GetMessage("ReadWriteSplittingPlugin.switchedFromWriterToReader", r.readerHostInfo.GetUrl()))

	return nil
}

func (r *ReadWriteSplittingPlugin) switchToWriterConnection(hosts []*host_info_util.HostInfo) error {
	currentConn := r.pluginService.GetCurrentConnection()
	currentHost, err := r.pluginService.GetCurrentHostInfo()
	if err != nil {
		return err
	}

	if r.isWriter(currentHost) && r.isConnectionUsable(currentConn) {
		return nil
	}

	writerHost := host_info_util.GetWriter(hosts)
	if writerHost == nil {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("ReadWriteSplittingPlugin.noWriterFound"))
	}

	r.inReadWriteSplit = true
	if !r.isConnectionUsable(r.writerConnection) {
		if err := r.getNewWriterConnection(writerHost); err != nil {
			return err
		}
	} else {
		err := r.switchCurrentConnectionTo(r.writerConnection, writerHost)
		if err != nil {
			return err
		}
	}

	slog.Info(error_util.GetMessage("ReadWriteSplittingPlugin.switchedFromReaderToWriter", writerHost.GetUrl()))
	return nil
}

func (r *ReadWriteSplittingPlugin) switchCurrentConnectionTo(newConn driver.Conn, newConnHost *host_info_util.HostInfo) error {
	currentConn := r.pluginService.GetCurrentConnection()
	if currentConn == newConn {
		return nil
	}

	slog.Debug(error_util.GetMessage("ReadWriteSplittingPlugin.settingCurrentConnection", newConnHost.GetUrl()))
	return r.pluginService.SetCurrentConnection(newConn, newConnHost, nil)
}

func (r *ReadWriteSplittingPlugin) initializeReaderConnection(hosts []*host_info_util.HostInfo) error {
	if len(hosts) == 1 {
		writerHost := host_info_util.GetWriter(hosts)
		if writerHost == nil {
			return error_util.NewGenericAwsWrapperError(error_util.GetMessage("ReadWriteSplittingPlugin.noWriterFound"))
		}
		if !r.isConnectionUsable(r.writerConnection) {
			return r.getNewWriterConnection(writerHost)
		}

		return nil
	}

	err := r.getNewReaderConnection()
	if err == nil && r.readerHostInfo != nil {
		slog.Info(error_util.GetMessage("ReadWriteSplittingPlugin.switchedFromWriterToReader", r.readerHostInfo.GetUrl()))
	}
	return err
}

func (r *ReadWriteSplittingPlugin) getNewWriterConnection(writerHost *host_info_util.HostInfo) error {
	conn, err := r.pluginService.Connect(writerHost, r.props, r)
	if err != nil {
		return err
	}
	r.setWriterConnection(conn, writerHost)
	return r.switchCurrentConnectionTo(r.writerConnection, r.writerHostInfo)
}

func (r *ReadWriteSplittingPlugin) getNewReaderConnection() error {
	hosts := r.pluginService.GetHosts()
	connAttempts := len(hosts) * 2
	var readerHost *host_info_util.HostInfo = nil
	var conn driver.Conn = nil
	for range connAttempts {
		hostInfo, err := r.pluginService.GetHostInfoByStrategy(host_info_util.READER, r.readerSelectorStrategy, hosts)
		if err != nil {
			if hostInfo != nil {
				slog.Warn(error_util.GetMessage("ReadWriteSplittingPlugin.failedToConnectToReader", hostInfo.GetUrl()))
			} else {
				slog.Warn(error_util.GetMessage("ReadWriteSplittingPlugin.failedToConnectToReader", "unknown host"))
			}
			continue
		}

		conn, err = r.pluginService.Connect(hostInfo, r.props, r)

		if err == nil {
			readerHost = hostInfo
			break
		}
	}

	if conn == nil || readerHost == nil {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("ReadWriteSplittingPlugin.noReadersAvailable"))
	}

	r.setReaderConnection(conn, readerHost)
	return r.switchCurrentConnectionTo(r.readerConnection, r.readerHostInfo)
}

func (r *ReadWriteSplittingPlugin) closeIdleConnections() {
	r.closeConnectionIfIdle(r.readerConnection)
	r.closeConnectionIfIdle(r.writerConnection)
}

func (r *ReadWriteSplittingPlugin) closeConnectionIfIdle(conn driver.Conn) {
	currentConn := r.pluginService.GetCurrentConnection()

	if conn != nil &&
		currentConn != conn {
		if !r.pluginService.GetTargetDriverDialect().IsClosed(conn) {
			conn.Close()
		}
		switch conn {
		case r.readerConnection:
			r.readerConnection = nil
			r.readerHostInfo = nil
		case r.writerConnection:
			r.writerConnection = nil
			r.writerHostInfo = nil
		}
	}
}

func (r ReadWriteSplittingPlugin) isConnectionUsable(conn driver.Conn) bool {
	return conn != nil && !r.pluginService.GetTargetDriverDialect().IsClosed(conn)
}

func (r ReadWriteSplittingPlugin) isWriter(hostInfo *host_info_util.HostInfo) bool {
	return hostInfo != nil && hostInfo.Role == host_info_util.WRITER
}

func (r ReadWriteSplittingPlugin) isReader(hostInfo *host_info_util.HostInfo) bool {
	return hostInfo != nil && hostInfo.Role == host_info_util.READER
}

func (r *ReadWriteSplittingPlugin) setWriterConnection(conn driver.Conn, host *host_info_util.HostInfo) {
	r.writerConnection = conn
	r.writerHostInfo = host
}

func (r *ReadWriteSplittingPlugin) setReaderConnection(conn driver.Conn, host *host_info_util.HostInfo) {
	r.readerConnection = conn
	r.readerHostInfo = host
}
