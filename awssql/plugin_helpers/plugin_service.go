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

package plugin_helpers

import (
	"context"
	"database/sql/driver"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

var hostAvailabilityExpiringCache = utils.NewCache[host_info_util.HostAvailability]()
var statusesExpiringCache = utils.NewCache[driver_infrastructure.BlueGreenStatus]()
var DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO = 5 * time.Minute
var DEFAULT_STATUS_CACHE_EXPIRE_NANO = 60 * time.Minute

type PluginServiceImpl struct {
	servicesContainer   driver_infrastructure.ServicesContainer
	driverDialect       driver_infrastructure.DriverDialect
	props               *utils.RWMap[string, string]
	currentConnection   *driver.Conn
	hostListProvider    driver_infrastructure.HostListProvider
	currentHostInfo     *host_info_util.HostInfo
	dialect             driver_infrastructure.DatabaseDialect
	dialectProvider     driver_infrastructure.DialectProvider
	isDialectConfirmed  bool
	originalDsn         string
	AllHosts            []*host_info_util.HostInfo
	allHostsLock        *sync.RWMutex
	initialHostInfo     *host_info_util.HostInfo
	isInTransaction     bool
	currentTx           driver.Tx
	sessionStateService driver_infrastructure.SessionStateService
}

func NewPluginServiceImpl(
	container driver_infrastructure.ServicesContainer,
	driverDialect driver_infrastructure.DriverDialect,
	props *utils.RWMap[string, string],
	dsn string,
) (driver_infrastructure.PluginService, error) {
	dialectProvider := driver_infrastructure.DialectManager{}
	dialect, err := dialectProvider.GetDialect(dsn, props)
	if err != nil {
		return nil, err
	}
	pluginService := &PluginServiceImpl{
		servicesContainer: container,
		driverDialect:     driverDialect,
		props:             props,
		dialectProvider:   &dialectProvider,
		dialect:           dialect,
		originalDsn:       dsn,
		allHostsLock:      new(sync.RWMutex),
	}
	sessionStateService := driver_infrastructure.NewSessionStateServiceImpl(pluginService, props)
	pluginService.sessionStateService = sessionStateService
	return pluginService, nil
}

func (p *PluginServiceImpl) IsStaticHostListProvider() bool {
	_, ok := p.GetHostListProvider().(driver_infrastructure.StaticHostListProvider)
	return ok
}

func (p *PluginServiceImpl) SetHostListProvider(hostListProvider driver_infrastructure.HostListProvider) {
	p.hostListProvider = hostListProvider
}

func (p *PluginServiceImpl) CreateHostListProvider(props *utils.RWMap[string, string]) driver_infrastructure.HostListProvider {
	return p.GetDialect().GetHostListProviderSupplier()(props, p.originalDsn, p.servicesContainer)
}

func (p *PluginServiceImpl) GetDialect() driver_infrastructure.DatabaseDialect {
	return p.dialect
}

func (p *PluginServiceImpl) SetDialect(dialect driver_infrastructure.DatabaseDialect) {
	p.dialect = dialect
}

func (p *PluginServiceImpl) IsDialectConfirmed() bool {
	return p.isDialectConfirmed
}

func (p *PluginServiceImpl) UpdateDialect(conn driver.Conn) {
	originalDialect := p.dialect
	p.dialect = p.dialectProvider.GetDialectForUpdate(conn, p.originalDsn, p.initialHostInfo.GetHost())
	p.isDialectConfirmed = true
	if originalDialect == p.dialect {
		return
	}
	p.SetHostListProvider(p.CreateHostListProvider(p.props))
	_ = p.RefreshHostList(conn)
}

func (p *PluginServiceImpl) GetCurrentConnection() driver.Conn {
	if p.currentConnection == nil {
		return nil
	}
	return *p.currentConnection
}

func (p *PluginServiceImpl) GetCurrentConnectionRef() *driver.Conn {
	return p.currentConnection
}

func (p *PluginServiceImpl) SetCurrentConnection(
	conn driver.Conn,
	hostInfo *host_info_util.HostInfo,
	skipNotificationForThisPlugin driver_infrastructure.ConnectionPlugin) error {
	if conn == nil {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("PluginServiceImpl.nilConn"))
	}
	if hostInfo == nil || hostInfo.IsNil() {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("PluginServiceImpl.nilHost"))
	}
	pluginManager := p.servicesContainer.GetPluginManager()
	if p.currentConnection == nil {
		// Setting up an initial connection.
		p.currentConnection = &conn
		p.currentHostInfo = hostInfo
		p.sessionStateService.Reset()

		if p.initialHostInfo == nil {
			p.initialHostInfo = hostInfo
		}

		changes := map[driver_infrastructure.HostChangeOptions]bool{
			driver_infrastructure.INITIAL_CONNECTION: true,
		}
		pluginManager.NotifyConnectionChanged(changes, skipNotificationForThisPlugin)
	} else {
		changes := p.compare(*p.currentConnection, p.currentHostInfo, conn, hostInfo)

		if len(changes) > 0 {
			err := p.sessionStateService.Begin()
			defer p.sessionStateService.Complete()
			if err != nil {
				return err
			}

			oldConnection := *p.currentConnection
			isInTransaction := p.IsInTransaction()

			p.currentConnection = &conn
			p.currentHostInfo = hostInfo
			err = p.sessionStateService.ApplyCurrentSessionState(conn)
			if err != nil {
				return err
			}
			p.SetInTransaction(false)

			if isInTransaction && property_util.GetVerifiedWrapperPropertyValue[bool](p.props, property_util.ROLLBACK_ON_SWITCH) {
				utils.Rollback(oldConnection, p.GetCurrentTx())
			}

			pluginOpinions := pluginManager.NotifyConnectionChanged(changes, skipNotificationForThisPlugin)
			_, connectionObjectHasChanged := changes[driver_infrastructure.CONNECTION_OBJECT_CHANGED]
			_, preserve := pluginOpinions[driver_infrastructure.PRESERVE]

			shouldCloseConnection := connectionObjectHasChanged && !p.GetTargetDriverDialect().IsClosed(oldConnection) && !preserve
			if shouldCloseConnection {
				_ = p.sessionStateService.ApplyPristineSessionState(oldConnection)
				_ = oldConnection.Close()
			}
		}
	}
	return nil
}

func (p *PluginServiceImpl) GetCurrentTx() driver.Tx {
	return p.currentTx
}

func (p *PluginServiceImpl) SetCurrentTx(tx driver.Tx) {
	p.currentTx = tx
}

func (p *PluginServiceImpl) compare(connA driver.Conn, hostInfoA *host_info_util.HostInfo, connB driver.Conn,
	hostInfoB *host_info_util.HostInfo) map[driver_infrastructure.HostChangeOptions]bool {
	changes := p.compareHostInfos(hostInfoA, hostInfoB)
	if connA != connB {
		changes[driver_infrastructure.CONNECTION_OBJECT_CHANGED] = true
	}
	return changes
}

func (p *PluginServiceImpl) compareHostInfos(hostInfoA *host_info_util.HostInfo, hostInfoB *host_info_util.HostInfo) map[driver_infrastructure.HostChangeOptions]bool {
	changes := map[driver_infrastructure.HostChangeOptions]bool{}
	if hostInfoA.Host != hostInfoB.Host || hostInfoA.Port != hostInfoB.Port {
		changes[driver_infrastructure.HOSTNAME] = true
	}
	if hostInfoA.Role != hostInfoB.Role {
		switch hostInfoB.Role {
		case host_info_util.WRITER:
			changes[driver_infrastructure.PROMOTED_TO_WRITER] = true
		case host_info_util.READER:
			changes[driver_infrastructure.PROMOTED_TO_READER] = true
		}
	}
	if hostInfoA.Availability != hostInfoB.Availability {
		switch hostInfoB.Availability {
		case host_info_util.AVAILABLE:
			changes[driver_infrastructure.WENT_UP] = true
		case host_info_util.UNAVAILABLE:
			changes[driver_infrastructure.WENT_DOWN] = true
		}
	}
	if len(changes) != 0 {
		changes[driver_infrastructure.HOST_CHANGED] = true
	}
	return changes
}

func (p *PluginServiceImpl) GetCurrentHostInfo() (*host_info_util.HostInfo, error) {
	if p.currentHostInfo.IsNil() {
		p.currentHostInfo = p.initialHostInfo
		if p.currentHostInfo.IsNil() {
			p.allHostsLock.RLock()
			defer p.allHostsLock.RUnlock()
			if len(p.AllHosts) == 0 {
				return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("PluginServiceImpl.hostListEmpty"))
			}

			p.currentHostInfo = host_info_util.GetWriter(p.AllHosts)
			allowedHosts := p.GetHosts()
			if p.currentHostInfo != nil && !host_info_util.IsHostInList(p.currentHostInfo, allowedHosts) {
				return nil, error_util.NewGenericAwsWrapperError(
					error_util.GetMessage("PluginServiceImpl.currentHostNotAllowed", p.currentHostInfo.GetHostAndPort(), utils.LogTopology(allowedHosts, "")))
			}

			if p.currentHostInfo.IsNil() {
				p.currentHostInfo = p.AllHosts[0]
			}
		}
		if p.currentHostInfo.IsNil() {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("PluginServiceImpl.nilHost"))
		}
		slog.Info(error_util.GetMessage("PluginServiceImpl.setCurrentHost", p.currentHostInfo.Host))
	}
	return p.currentHostInfo, nil
}

func (p *PluginServiceImpl) GetAllHosts() []*host_info_util.HostInfo {
	p.allHostsLock.RLock()
	defer p.allHostsLock.RUnlock()
	return p.AllHosts
}

func (p *PluginServiceImpl) GetHosts() []*host_info_util.HostInfo {
	p.allHostsLock.RLock()
	defer p.allHostsLock.RUnlock()

	hostPermissions, found := driver_infrastructure.AllowedAndBlockedHostsStorageType.Get(p.servicesContainer.GetStorageService(), p.initialHostInfo.GetUrl())
	if !found {
		return p.AllHosts
	}

	hosts := p.AllHosts
	allowedHosts := hostPermissions.GetAllowedHostIds()
	blockedHosts := hostPermissions.GetBlockedHostIds()

	if len(allowedHosts) > 0 {
		hosts = utils.FilterSlice(hosts, func(item *host_info_util.HostInfo) bool {
			value, ok := allowedHosts[item.HostId]
			return ok && value
		})
	}

	if len(blockedHosts) > 0 {
		hosts = utils.FilterSlice(hosts, func(item *host_info_util.HostInfo) bool {
			value, ok := blockedHosts[item.HostId]
			return !ok || !value
		})
	}

	return hosts
}

func (p *PluginServiceImpl) GetInitialConnectionHostInfo() *host_info_util.HostInfo {
	return p.initialHostInfo
}

func (p *PluginServiceImpl) AcceptsStrategy(strategy string) bool {
	return p.servicesContainer.GetPluginManager().AcceptsStrategy(strategy)
}

func (p *PluginServiceImpl) GetHostInfoByStrategy(
	role host_info_util.HostRole,
	strategy string,
	hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	return p.servicesContainer.GetPluginManager().GetHostInfoByStrategy(role, strategy, hosts)
}

func (p *PluginServiceImpl) GetHostSelectorStrategy(strategy string) (hostSelector driver_infrastructure.HostSelector, err error) {
	return p.servicesContainer.GetPluginManager().GetHostSelectorStrategy(strategy)
}

func (p *PluginServiceImpl) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return p.hostListProvider.GetHostRole(conn)
}

func (p *PluginServiceImpl) SetAvailability(hostAliases map[string]bool, availability host_info_util.HostAvailability) {
	if len(hostAliases) == 0 {
		return
	}

	changes := map[string]map[driver_infrastructure.HostChangeOptions]bool{}
	hostsToChange := false

	p.allHostsLock.Lock()
	defer p.allHostsLock.Unlock()
	for i, host := range p.AllHosts {
		hostAliasesAsSlice := utils.AllKeys(hostAliases)
		if slices.Contains(hostAliasesAsSlice, host.GetHostAndPort()) || utils.SliceAndMapHaveCommonElement(hostAliasesAsSlice, host.AllAliases) {
			hostsToChange = true
			currentAvailability := host.Availability
			hostAvailabilityExpiringCache.Put(host.GetHostAndPort(), availability, DEFAULT_HOST_AVAILABILITY_CACHE_EXPIRE_NANO)
			if currentAvailability != availability {
				// Set the host in AllHosts to the new availability.
				p.AllHosts[i].Availability = availability

				// Determine which changes were made to update the plugin manager.
				var hostChanges map[driver_infrastructure.HostChangeOptions]bool
				if availability == host_info_util.AVAILABLE {
					hostChanges = map[driver_infrastructure.HostChangeOptions]bool{
						driver_infrastructure.HOST_CHANGED: true,
						driver_infrastructure.WENT_UP:      true,
					}
				} else {
					hostChanges = map[driver_infrastructure.HostChangeOptions]bool{
						driver_infrastructure.HOST_CHANGED: true,
						driver_infrastructure.WENT_DOWN:    true,
					}
				}
				changes[host.GetHostAndPort()] = hostChanges
			}
		}
	}

	if !hostsToChange {
		slog.Info(error_util.GetMessage("PluginServiceImpl.hostsChangelistEmpty"))
	}

	if len(changes) > 0 {
		p.servicesContainer.GetPluginManager().NotifyHostListChanged(changes)
	}
}

func (p *PluginServiceImpl) IsInTransaction() bool {
	return p.isInTransaction
}

func (p *PluginServiceImpl) SetInTransaction(inTransaction bool) {
	p.isInTransaction = inTransaction
}

func (p *PluginServiceImpl) GetHostListProvider() driver_infrastructure.HostListProvider {
	return p.hostListProvider
}

func (p *PluginServiceImpl) RefreshHostList(conn driver.Conn) error {
	updatedHostList, err := p.GetHostListProvider().Refresh()
	if err != nil {
		return err
	}
	return p.updateHostListIfNeeded(updatedHostList)
}

func (p *PluginServiceImpl) ForceRefreshHostList(conn driver.Conn) error {
	updatedHostList, err := p.GetHostListProvider().ForceRefresh()
	if err != nil {
		return err
	}
	return p.updateHostListIfNeeded(updatedHostList)
}

func (p *PluginServiceImpl) updateHostListIfNeeded(updatedHostList []*host_info_util.HostInfo) error {
	if len(updatedHostList) == 0 {
		return error_util.NewGenericAwsWrapperError(error_util.GetMessage("PluginServiceImpl.hostListEmpty"))
	}
	if !host_info_util.AreHostListsEqual(p.AllHosts, updatedHostList) {
		p.updateHostAvailability(updatedHostList)
		p.setHostList(updatedHostList)
	}
	return nil
}

func (p *PluginServiceImpl) setHostList(newHosts []*host_info_util.HostInfo) {
	p.allHostsLock.Lock()
	defer p.allHostsLock.Unlock()
	var oldHostMap = map[string]*host_info_util.HostInfo{}
	for _, host := range p.AllHosts {
		oldHostMap[host.GetHostAndPort()] = host
	}

	var newHostMap = map[string]*host_info_util.HostInfo{}
	for _, host := range newHosts {
		newHostMap[host.GetHostAndPort()] = host
	}

	var changes = map[string]map[driver_infrastructure.HostChangeOptions]bool{}
	for hostKey, hostInfo := range oldHostMap {
		correspondingNewHost, ok := newHostMap[hostKey]
		if !ok || correspondingNewHost.IsNil() {
			// Host has been deleted.
			changes[hostKey] = map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.HOST_DELETED: true}
		} else {
			// Host maybe changed.
			hostChanges := p.compareHostInfos(hostInfo, correspondingNewHost)
			if len(hostChanges) > 0 {
				changes[hostKey] = hostChanges
			}
		}
	}
	for hostKey := range newHostMap {
		_, oldHostMapContainsKey := oldHostMap[hostKey]
		if !oldHostMapContainsKey {
			// Host has been added.
			changes[hostKey] = map[driver_infrastructure.HostChangeOptions]bool{driver_infrastructure.HOST_ADDED: true}
		}
	}

	if len(changes) > 0 {
		p.AllHosts = newHosts
		p.servicesContainer.GetPluginManager().NotifyHostListChanged(changes)
	}
}

func (p *PluginServiceImpl) updateHostAvailability(hosts []*host_info_util.HostInfo) {
	for _, host := range hosts {
		availability, ok := hostAvailabilityExpiringCache.Get(host.GetHostAndPort())
		if ok {
			host.Availability = availability
		}
	}
}

func (p *PluginServiceImpl) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	pluginToSkip driver_infrastructure.ConnectionPlugin) (driver.Conn, error) {
	return p.servicesContainer.GetPluginManager().Connect(hostInfo, props, p.currentConnection == nil, pluginToSkip)
}

func (p *PluginServiceImpl) ForceConnect(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (driver.Conn, error) {
	return p.servicesContainer.GetPluginManager().ForceConnect(hostInfo, props, p.currentConnection == nil)
}

func (p *PluginServiceImpl) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) (bool, error) {
	updatedHostList, err := p.GetUpdatedHostListWithTimeout(shouldVerifyWriter, timeoutMs)
	if err != nil {
		slog.Warn(err.Error())
		return false, err
	}
	if len(updatedHostList) != 0 {
		p.updateHostAvailability(updatedHostList)
		p.setHostList(updatedHostList)
		return true, nil
	}

	return false, nil
}

func (p *PluginServiceImpl) GetUpdatedHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	hostListProvider := p.GetHostListProvider()
	blockingHostListProvider, ok := hostListProvider.(driver_infrastructure.BlockingHostListProvider)
	if !ok {
		return nil, error_util.NewFailoverFailedError(error_util.GetMessage("PluginServiceImpl.requiredBlockingHostListProvider", hostListProvider))
	}

	return blockingHostListProvider.ForceRefreshHostListWithTimeout(shouldVerifyWriter, timeoutMs)
}

func (p *PluginServiceImpl) GetTargetDriverDialect() driver_infrastructure.DriverDialect {
	return p.driverDialect
}

func (p *PluginServiceImpl) IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error) {
	return p.hostListProvider.IdentifyConnection(conn)
}

func (p *PluginServiceImpl) FillAliases(conn driver.Conn, hostInfo *host_info_util.HostInfo) {
	if hostInfo.IsNil() {
		return
	}

	if len(hostInfo.Aliases) > 0 {
		slog.Info(error_util.GetMessage("PluginServiceImpl.nonEmptyAliases", hostInfo.AllAliases))
		return
	}

	// Add the hostname and port, this host name is usually the internal IP address.
	hostInfo.AddAlias(hostInfo.GetHostAndPort())

	queryer, ok := conn.(driver.QueryerContext)
	if ok {
		rows, err := queryer.QueryContext(context.Background(), p.dialect.GetHostAliasQuery(), nil)
		if err == nil && len(rows.Columns()) > 0 {
			driverValues := make([]driver.Value, len(rows.Columns()))
			for rows.Next(driverValues) == nil {
				valueAsString, ok := utils.ConvertDriverValueToString(driverValues[0])
				if ok {
					hostInfo.AddAlias(valueAsString)
				}
			}
		} else {
			slog.Info(error_util.GetMessage("PluginServiceImpl.failedToRetrieveHostPort"))
		}
	} else {
		slog.Info(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
		return
	}

	host, err := p.IdentifyConnection(conn)
	if err == nil && !host.IsNil() {
		for alias := range host.AllAliases {
			hostInfo.AllAliases[alias] = true
		}
	}
}

func (p *PluginServiceImpl) GetConnectionProvider() driver_infrastructure.ConnectionProvider {
	return p.servicesContainer.GetConnectionProvider()
}

func (p *PluginServiceImpl) GetProperties() *utils.RWMap[string, string] {
	return p.props
}

func (p *PluginServiceImpl) SetInitialConnectionHostInfo(hostInfo *host_info_util.HostInfo) {
	p.initialHostInfo = hostInfo
}

func (p *PluginServiceImpl) GetTelemetryContext() context.Context {
	return p.servicesContainer.GetPluginManager().GetTelemetryContext()
}

func (p *PluginServiceImpl) GetTelemetryFactory() telemetry.TelemetryFactory {
	return p.servicesContainer.GetPluginManager().GetTelemetryFactory()
}

func (p *PluginServiceImpl) SetTelemetryContext(ctx context.Context) {
	p.servicesContainer.GetPluginManager().SetTelemetryContext(ctx)
}

func (p *PluginServiceImpl) IsNetworkError(err error) bool {
	return p.driverDialect.IsNetworkError(err)
}

func (p *PluginServiceImpl) IsLoginError(err error) bool {
	return p.driverDialect.IsLoginError(err)
}

func (p *PluginServiceImpl) UpdateState(sql string, methodArgs ...any) {
	query := utils.GetQueryFromSqlOrMethodArgs(sql, methodArgs...)

	var autoCommit, updateAutoCommitOk, readOnly, updateReadOnlyOk, updateSchemaOk, updateCatalogOk, updateTransactionIsolationOk bool
	var schema, catalog string
	var transactionIsolation driver_infrastructure.TransactionIsolationLevel

	statements := utils.GetSeparateSqlStatements(query)
	for i := len(statements) - 1; i >= 0 && !updateAutoCommitOk && !updateReadOnlyOk && !updateSchemaOk && !updateCatalogOk && !updateTransactionIsolationOk; i-- {
		autoCommit, updateAutoCommitOk = p.dialect.DoesStatementSetAutoCommit(statements[i])
		readOnly, updateReadOnlyOk = p.dialect.DoesStatementSetReadOnly(statements[i])
		schema, updateSchemaOk = p.dialect.DoesStatementSetSchema(statements[i])
		catalog, updateCatalogOk = p.dialect.DoesStatementSetCatalog(statements[i])
		transactionIsolation, updateTransactionIsolationOk = p.dialect.DoesStatementSetTransactionIsolation(statements[i])
	}

	setIfOk := func(setter func(), ok bool) {
		if ok {
			setter()
		}
	}
	setIfOk(func() {
		p.sessionStateService.SetupPristineAutoCommit()
		p.sessionStateService.SetAutoCommit(autoCommit)
	}, updateAutoCommitOk)
	setIfOk(func() { p.sessionStateService.SetupPristineReadOnly(); p.sessionStateService.SetReadOnly(readOnly) }, updateReadOnlyOk)
	setIfOk(func() { p.sessionStateService.SetupPristineSchema(); p.sessionStateService.SetSchema(schema) }, updateSchemaOk)
	setIfOk(func() { p.sessionStateService.SetupPristineCatalog(); p.sessionStateService.SetCatalog(catalog) }, updateCatalogOk)
	setIfOk(func() {
		p.sessionStateService.SetupPristineTransactionIsolation()
		p.sessionStateService.SetTransactionIsolation(transactionIsolation)
	}, updateTransactionIsolationOk)
}

func (p *PluginServiceImpl) ReleaseResources() {
	slog.Debug(error_util.GetMessage("PluginServiceImpl.releaseResources"))
	if p.currentConnection != nil {
		_ = (*p.currentConnection).Close() // Ignore any error.
		p.currentConnection = nil
	}

	if p.hostListProvider != nil {
		canReleaseResources, ok := p.hostListProvider.(driver_infrastructure.CanReleaseResources)

		if ok {
			canReleaseResources.ReleaseResources()
		}
	}
}

func (p *PluginServiceImpl) ResetSession() {
	p.sessionStateService.Reset()
}

func (p *PluginServiceImpl) IsPluginInUse(pluginName string) bool {
	return p.servicesContainer.GetPluginManager().IsPluginInUse(pluginName)
}

// This cleans up all long-standing caches. To be called at the end of program, not each time a Conn is closed.
func ClearCaches() {
	if hostAvailabilityExpiringCache != nil {
		hostAvailabilityExpiringCache.Clear()
	}
}

func (p *PluginServiceImpl) CreatePartialPluginService() driver_infrastructure.PluginService {
	p.allHostsLock.RLock()
	defer p.allHostsLock.RUnlock()
	return NewPartialPluginService(p.servicesContainer, p.props, p.originalDsn, p.hostListProvider, p.dialect, p.driverDialect, p.AllHosts, p.allHostsLock, p.initialHostInfo)
}
