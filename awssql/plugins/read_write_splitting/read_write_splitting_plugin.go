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
	"errors"
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

func (factory ReadWriteSplittingPluginFactory) GetInstance(servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewReadWriteSplittingPlugin(
		servicesContainer, props,
		driver_infrastructure.READ_WRITE_SPLITTING_PLUGIN_CODE,
		&RdsReadWriteSplittingStrategy{}), nil
}

func (factory ReadWriteSplittingPluginFactory) ClearCaches() {
}

func NewReadWriteSplittingPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return ReadWriteSplittingPluginFactory{}
}

// ReadWriteSplittingStrategy defines how reader and writer connections are obtained.
type ReadWriteSplittingStrategy interface {
	// OnConnect is called during Connect to allow strategy-specific initialization.
	OnConnect(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) error

	// GetReaderConnection returns a new reader connection.
	GetReaderConnection(
		servicesContainer driver_infrastructure.ServicesContainer,
		props *utils.RWMap[string, string],
		hosts []*host_info_util.HostInfo,
		readerSelectorStrategy string,
		pluginToSkip driver_infrastructure.ConnectionPlugin,
	) (driver.Conn, *host_info_util.HostInfo, error)

	// GetWriterConnection returns a new writer connection.
	GetWriterConnection(
		servicesContainer driver_infrastructure.ServicesContainer,
		props *utils.RWMap[string, string],
		hosts []*host_info_util.HostInfo,
		pluginToSkip driver_infrastructure.ConnectionPlugin,
	) (driver.Conn, *host_info_util.HostInfo, error)
}

type ReadWriteSplittingPlugin struct {
	plugins.BaseConnectionPlugin
	servicesContainer       driver_infrastructure.ServicesContainer
	props                   *utils.RWMap[string, string]
	readerSelectorStrategy  string
	hostListProviderService driver_infrastructure.HostListProviderService
	inReadWriteSplit        bool
	writerConnection        driver.Conn
	readerConnection        driver.Conn
	writerHostInfo          *host_info_util.HostInfo
	readerHostInfo          *host_info_util.HostInfo
	strategy                ReadWriteSplittingStrategy
	pluginCode              string
}

func NewReadWriteSplittingPlugin(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
	pluginCode string,
	strategy ReadWriteSplittingStrategy,
) *ReadWriteSplittingPlugin {
	return &ReadWriteSplittingPlugin{
		servicesContainer: servicesContainer,
		props:             props,
		readerSelectorStrategy: property_util.GetVerifiedWrapperPropertyValue[string](props,
			property_util.READER_HOST_SELECTOR_STRATEGY),
		pluginCode: pluginCode,
		strategy:   strategy,
	}
}

func (r *ReadWriteSplittingPlugin) GetPluginCode() string {
	return r.pluginCode
}

func (r *ReadWriteSplittingPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD,
		plugin_helpers.INIT_HOST_PROVIDER_METHOD,
		plugin_helpers.NOTIFY_CONNECTION_CHANGED_METHOD,
		plugin_helpers.SET_READ_ONLY_METHOD,
		utils.CONN_QUERY_CONTEXT,
		utils.CONN_EXEC_CONTEXT,
	}
}

func (r *ReadWriteSplittingPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	if err := r.strategy.OnConnect(hostInfo, props); err != nil {
		return nil, err
	}

	if !r.servicesContainer.GetPluginService().AcceptsStrategy(r.readerSelectorStrategy) {
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

	currentRole := r.servicesContainer.GetPluginService().GetHostRole(result)
	if currentRole == host_info_util.UNKNOWN {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ReadWriteSplittingPlugin.errorVerifyingInitialHostRole"))
	}

	currentHost := r.servicesContainer.GetPluginService().GetInitialConnectionHostInfo()

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
	_ *utils.RWMap[string, string],
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

	if _, ok := changes[driver_infrastructure.CONNECTION_OBJECT_CHANGED]; ok {
		currentConn := r.servicesContainer.GetPluginService().GetCurrentConnection()
		isCachedConnection := currentConn == r.writerConnection || currentConn == r.readerConnection
		if !isCachedConnection {
			r.closeIdleConnections()
		}
	}

	if r.inReadWriteSplit {
		return driver_infrastructure.PRESERVE
	}
	return driver_infrastructure.NO_OPINION
}

func (r *ReadWriteSplittingPlugin) updateInternalConnectionInfo() error {
	currentConn := r.servicesContainer.GetPluginService().GetCurrentConnection()
	currentHost, err := r.servicesContainer.GetPluginService().GetCurrentHostInfo()

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
	_ driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (wrappedReturnValue any, wrappedReturnValue2 any, wrappedOk bool, wrappedErr error) {
	if methodName == plugin_helpers.SET_READ_ONLY_METHOD && len(methodArgs) > 0 {
		if readOnly, ok := methodArgs[0].(bool); ok {
			err := r.switchConnectionIfRequired(readOnly)
			if err != nil {
				r.closeIdleConnections()
				return nil, nil, false, err
			}
			return executeFunc()
		}
	}

	wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr = executeFunc()

	var awsWrapperError *error_util.AwsWrapperError
	ok := errors.As(wrappedErr, &awsWrapperError)

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
	currentConn := r.servicesContainer.GetPluginService().GetCurrentConnection()
	if currentConn != nil && r.servicesContainer.GetPluginService().GetTargetDriverDialect().IsClosed(currentConn) {
		return error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ReadWriteSplittingPlugin.setReadOnlyOnClosedConnection"))
	}

	if r.isConnectionUsable(currentConn) {
		err := r.servicesContainer.GetPluginService().RefreshHostList(currentConn)
		if err != nil {
			// ignore
			slog.Debug(error_util.GetMessage("ReadWriteSplittingPlugin.couldNotRefreshHostlist"))
		}
	}

	hosts := r.servicesContainer.GetPluginService().GetHosts()

	if len(hosts) == 0 {
		return error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("ReadWriteSplittingPlugin.emptyHostList"))
	}

	currentHost, err := r.servicesContainer.GetPluginService().GetCurrentHostInfo()
	if err != nil {
		return err
	}

	if readOnly {
		if !r.servicesContainer.GetPluginService().IsInTransaction() && !r.isReader(currentHost) {
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
	if !r.isWriter(currentHost) && r.servicesContainer.GetPluginService().IsInTransaction() {
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
	currentConn := r.servicesContainer.GetPluginService().GetCurrentConnection()
	currentHost, err := r.servicesContainer.GetPluginService().GetCurrentHostInfo()
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
		_ = r.readerConnection.Close()
		r.readerConnection = nil
		r.readerHostInfo = nil
		return r.initializeReaderConnection(hosts)
	}
	slog.Info(error_util.GetMessage("ReadWriteSplittingPlugin.switchedFromWriterToReader", r.readerHostInfo.GetUrl()))

	return nil
}

func (r *ReadWriteSplittingPlugin) switchToWriterConnection(hosts []*host_info_util.HostInfo) error {
	currentConn := r.servicesContainer.GetPluginService().GetCurrentConnection()
	currentHost, err := r.servicesContainer.GetPluginService().GetCurrentHostInfo()
	if err != nil {
		return err
	}

	if r.isWriter(currentHost) && r.isConnectionUsable(currentConn) {
		return nil
	}

	r.inReadWriteSplit = true
	if !r.isConnectionUsable(r.writerConnection) {
		if err := r.getNewWriterConnection(hosts); err != nil {
			return err
		}
	} else {
		err := r.switchCurrentConnectionTo(r.writerConnection, r.writerHostInfo)
		if err != nil {
			return err
		}
	}

	slog.Info(error_util.GetMessage("ReadWriteSplittingPlugin.switchedFromReaderToWriter", r.writerHostInfo.GetUrl()))
	return nil
}

func (r *ReadWriteSplittingPlugin) switchCurrentConnectionTo(newConn driver.Conn, newConnHost *host_info_util.HostInfo) error {
	currentConn := r.servicesContainer.GetPluginService().GetCurrentConnection()
	if currentConn == newConn {
		return nil
	}

	slog.Debug(error_util.GetMessage("ReadWriteSplittingPlugin.settingCurrentConnection", newConnHost.GetUrl()))
	return r.servicesContainer.GetPluginService().SetCurrentConnection(newConn, newConnHost, nil)
}

func (r *ReadWriteSplittingPlugin) initializeReaderConnection(hosts []*host_info_util.HostInfo) error {
	if len(hosts) == 1 {
		writerHost := host_info_util.GetWriter(hosts)
		if writerHost == nil {
			return error_util.NewGenericAwsWrapperError(error_util.GetMessage("ReadWriteSplittingPlugin.noWriterFound"))
		}
		if !r.isConnectionUsable(r.writerConnection) {
			return r.getNewWriterConnection(hosts)
		}

		return nil
	}

	err := r.getNewReaderConnection(hosts)
	if err == nil && r.readerHostInfo != nil {
		slog.Info(error_util.GetMessage("ReadWriteSplittingPlugin.switchedFromWriterToReader", r.readerHostInfo.GetUrl()))
	}
	return err
}

func (r *ReadWriteSplittingPlugin) getNewWriterConnection(hosts []*host_info_util.HostInfo) error {
	conn, host, err := r.strategy.GetWriterConnection(r.servicesContainer, r.props, hosts, r)
	if err != nil {
		return err
	}
	r.setWriterConnection(conn, host)
	return r.switchCurrentConnectionTo(r.writerConnection, r.writerHostInfo)
}

func (r *ReadWriteSplittingPlugin) getNewReaderConnection(hosts []*host_info_util.HostInfo) error {
	conn, host, err := r.strategy.GetReaderConnection(r.servicesContainer, r.props, hosts, r.readerSelectorStrategy, r)
	if err != nil {
		return err
	}
	r.setReaderConnection(conn, host)
	return r.switchCurrentConnectionTo(r.readerConnection, r.readerHostInfo)
}

func (r *ReadWriteSplittingPlugin) closeIdleConnections() {
	r.closeConnectionIfIdle(r.readerConnection)
	r.closeConnectionIfIdle(r.writerConnection)
}

func (r *ReadWriteSplittingPlugin) closeConnectionIfIdle(conn driver.Conn) {
	currentConn := r.servicesContainer.GetPluginService().GetCurrentConnection()

	if conn != nil &&
		currentConn != conn {
		if !r.servicesContainer.GetPluginService().GetTargetDriverDialect().IsClosed(conn) {
			_ = conn.Close()
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

func (r *ReadWriteSplittingPlugin) isConnectionUsable(conn driver.Conn) bool {
	return conn != nil && !r.servicesContainer.GetPluginService().GetTargetDriverDialect().IsClosed(conn)
}

func (r *ReadWriteSplittingPlugin) isWriter(hostInfo *host_info_util.HostInfo) bool {
	return hostInfo != nil && hostInfo.Role == host_info_util.WRITER
}

func (r *ReadWriteSplittingPlugin) isReader(hostInfo *host_info_util.HostInfo) bool {
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

type RdsReadWriteSplittingStrategy struct{}

func (s *RdsReadWriteSplittingStrategy) OnConnect(
	_ *host_info_util.HostInfo,
	_ *utils.RWMap[string, string],
) error {
	return nil
}

func (s *RdsReadWriteSplittingStrategy) GetWriterConnection(
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

func (s *RdsReadWriteSplittingStrategy) GetReaderConnection(
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
				slog.Warn(error_util.GetMessage("ReadWriteSplittingPlugin.failedToSelectReaderHost", hostInfo.GetUrl()))
			} else {
				slog.Warn(error_util.GetMessage("ReadWriteSplittingPlugin.failedToSelectReaderHost", "unknown host"))
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
