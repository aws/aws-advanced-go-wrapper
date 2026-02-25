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
	"sync/atomic"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

// This class is meant to be a lightweight PluginService implementation to be used by multithreaded structs that do not need the full functionality of the PluginServiceImpl.
// To keep it light, some fields/methods are not implemented. `panic()` is used to ensure that these methods not used and are caught during development/testing.
// If the PartialPluginService fits your usecase but doesn't implement the field/method you need, feel free to add it.
type PartialPluginService struct {
	servicesContainer      driver_infrastructure.ServicesContainer
	props                  *utils.RWMap[string, string]
	originalDsn            string
	hostListProvider       driver_infrastructure.HostListProvider
	dialect                driver_infrastructure.DatabaseDialect
	driverDialect          driver_infrastructure.DriverDialect
	AllHosts               []*host_info_util.HostInfo
	allHostsLock           *sync.RWMutex
	allowedAndBlockedHosts *atomic.Pointer[driver_infrastructure.AllowedAndBlockedHosts]
}

func NewPartialPluginService(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
	originalDsn string,
	hostListProvider driver_infrastructure.HostListProvider,
	dialect driver_infrastructure.DatabaseDialect,
	driverDialect driver_infrastructure.DriverDialect,
	AllHosts []*host_info_util.HostInfo,
	allHostsLock *sync.RWMutex,
	allowedAndBlockedHosts *atomic.Pointer[driver_infrastructure.AllowedAndBlockedHosts]) driver_infrastructure.PluginService {
	return &PartialPluginService{
		servicesContainer:      servicesContainer,
		props:                  props,
		originalDsn:            originalDsn,
		hostListProvider:       hostListProvider,
		dialect:                dialect,
		driverDialect:          driverDialect,
		AllHosts:               AllHosts,
		allHostsLock:           allHostsLock,
		allowedAndBlockedHosts: allowedAndBlockedHosts,
	}
}

func (p *PartialPluginService) ForceConnect(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string]) (driver.Conn, error) {
	return p.servicesContainer.GetPluginManager().ForceConnect(hostInfo, props, true)
}

func (p *PartialPluginService) GetHostSelectorStrategy(strategy string) (hostSelector driver_infrastructure.HostSelector, err error) {
	return p.servicesContainer.GetPluginManager().GetHostSelectorStrategy(strategy)
}

func (p *PartialPluginService) GetTargetDriverDialect() driver_infrastructure.DriverDialect {
	return p.driverDialect
}

func (p *PartialPluginService) GetDialect() driver_infrastructure.DatabaseDialect {
	return p.dialect
}

func (p *PartialPluginService) CreateHostListProvider(props *utils.RWMap[string, string]) driver_infrastructure.HostListProvider {
	supplier := p.GetDialect().GetHostListProviderSupplier()
	if supplier != nil {
		return supplier(props, p.originalDsn, p.servicesContainer)
	}
	return nil
}

func (p *PartialPluginService) GetTelemetryContext() context.Context {
	return p.servicesContainer.GetPluginManager().GetTelemetryContext()
}

func (p *PartialPluginService) SetTelemetryContext(ctx context.Context) {
	p.servicesContainer.GetPluginManager().SetTelemetryContext(ctx)
}

func (p *PartialPluginService) SetAllowedAndBlockedHosts(allowedAndBlockedHosts *driver_infrastructure.AllowedAndBlockedHosts) {
	p.allowedAndBlockedHosts.Store(allowedAndBlockedHosts)
}

func (p *PartialPluginService) SetAvailability(hostAliases map[string]bool, availability host_info_util.HostAvailability) {
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

func (p *PartialPluginService) GetAllHosts() []*host_info_util.HostInfo {
	p.allHostsLock.RLock()
	defer p.allHostsLock.RUnlock()
	return p.AllHosts
}
func (p *PartialPluginService) GetHosts() []*host_info_util.HostInfo {
	p.allHostsLock.RLock()
	defer p.allHostsLock.RUnlock()

	hostPermissions := p.allowedAndBlockedHosts.Load()
	if hostPermissions == nil {
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

func (p *PartialPluginService) CreatePartialPluginService() driver_infrastructure.PluginService {
	return p
}

func (p *PartialPluginService) GetCurrentConnection() driver.Conn { panic("Method not implemented.") }
func (p *PartialPluginService) GetCurrentConnectionRef() *driver.Conn {
	panic("Method not implemented.")
}
func (p *PartialPluginService) SetCurrentConnection(
	conn driver.Conn, hostInfo *host_info_util.HostInfo, skipNotificationForThisPlugin driver_infrastructure.ConnectionPlugin) error {
	panic("Method not implemented.")
}
func (p *PartialPluginService) GetInitialConnectionHostInfo() *host_info_util.HostInfo {
	panic("Method not implemented.")
}
func (p *PartialPluginService) GetCurrentHostInfo() (*host_info_util.HostInfo, error) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) AcceptsStrategy(strategy string) bool {
	panic("Method not implemented.")
}
func (p *PartialPluginService) GetHostInfoByStrategy(role host_info_util.HostRole, strategy string, hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) GetHostRole(driver.Conn) host_info_util.HostRole {
	panic("Method not implemented.")
}
func (p *PartialPluginService) IsInTransaction() bool               { panic("Method not implemented.") }
func (p *PartialPluginService) SetInTransaction(inTransaction bool) { panic("Method not implemented.") }
func (p *PartialPluginService) GetCurrentTx() driver.Tx             { panic("Method not implemented.") }
func (p *PartialPluginService) SetCurrentTx(driver.Tx)              { panic("Method not implemented.") }
func (p *PartialPluginService) SetHostListProvider(hostListProvider driver_infrastructure.HostListProvider) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) SetInitialConnectionHostInfo(info *host_info_util.HostInfo) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) IsStaticHostListProvider() bool { panic("Method not implemented.") }
func (p *PartialPluginService) GetHostListProvider() driver_infrastructure.HostListProvider {
	panic("Method not implemented.")
}
func (p *PartialPluginService) RefreshHostList(conn driver.Conn) error {
	panic("Method not implemented.")
}
func (p *PartialPluginService) ForceRefreshHostList(conn driver.Conn) error {
	panic("Method not implemented.")
}
func (p *PartialPluginService) ForceRefreshHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) (bool, error) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) GetUpdatedHostListWithTimeout(shouldVerifyWriter bool, timeoutMs int) ([]*host_info_util.HostInfo, error) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) Connect(
	hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], pluginToSkip driver_infrastructure.ConnectionPlugin) (driver.Conn, error) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) SetDialect(dialect driver_infrastructure.DatabaseDialect) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) IsDialectConfirmed() bool       { panic("Method not implemented.") }
func (p *PartialPluginService) UpdateDialect(conn driver.Conn) { panic("Method not implemented.") }
func (p *PartialPluginService) IdentifyConnection(conn driver.Conn) (*host_info_util.HostInfo, error) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) FillAliases(conn driver.Conn, hostInfo *host_info_util.HostInfo) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) GetConnectionProvider() driver_infrastructure.ConnectionProvider {
	panic("Method not implemented.")
}
func (p *PartialPluginService) GetProperties() *utils.RWMap[string, string] {
	panic("Method not implemented.")
}
func (p *PartialPluginService) IsNetworkError(err error) bool { panic("Method not implemented.") }
func (p *PartialPluginService) IsLoginError(err error) bool   { panic("Method not implemented.") }
func (p *PartialPluginService) GetTelemetryFactory() telemetry.TelemetryFactory {
	panic("Method not implemented.")
}
func (p *PartialPluginService) UpdateState(sql string, methodArgs ...any) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) GetBgStatus(id string) (driver_infrastructure.BlueGreenStatus, bool) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) SetBgStatus(status driver_infrastructure.BlueGreenStatus, id string) {
	panic("Method not implemented.")
}
func (p *PartialPluginService) IsPluginInUse(pluginName string) bool {
	panic("Method not implemented.")
}
func (p *PartialPluginService) ResetSession() { panic("Method not implemented.") }
