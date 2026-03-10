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
	"context"
	"database/sql/driver"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

// GlobalDbFailoverMode represents the failover mode for Global Aurora Databases.
type GlobalDbFailoverMode string

const (
	GDB_MODE_STRICT_WRITER                GlobalDbFailoverMode = "strict-writer"
	GDB_MODE_STRICT_HOME_READER           GlobalDbFailoverMode = "strict-home-reader"
	GDB_MODE_STRICT_OUT_OF_HOME_READER    GlobalDbFailoverMode = "strict-out-of-home-reader"
	GDB_MODE_STRICT_ANY_READER            GlobalDbFailoverMode = "strict-any-reader"
	GDB_MODE_HOME_READER_OR_WRITER        GlobalDbFailoverMode = "home-reader-or-writer"
	GDB_MODE_OUT_OF_HOME_READER_OR_WRITER GlobalDbFailoverMode = "out-of-home-reader-or-writer"
	GDB_MODE_ANY_READER_OR_WRITER         GlobalDbFailoverMode = "any-reader-or-writer"
	GDB_MODE_UNKNOWN                      GlobalDbFailoverMode = ""
)

func gdbFailoverModeFromValue(value string) GlobalDbFailoverMode {
	switch strings.ToLower(value) {
	case "strict-writer":
		return GDB_MODE_STRICT_WRITER
	case "strict-home-reader":
		return GDB_MODE_STRICT_HOME_READER
	case "strict-out-of-home-reader":
		return GDB_MODE_STRICT_OUT_OF_HOME_READER
	case "strict-any-reader":
		return GDB_MODE_STRICT_ANY_READER
	case "home-reader-or-writer":
		return GDB_MODE_HOME_READER_OR_WRITER
	case "out-of-home-reader-or-writer":
		return GDB_MODE_OUT_OF_HOME_READER_OR_WRITER
	case "any-reader-or-writer":
		return GDB_MODE_ANY_READER_OR_WRITER
	default:
		return GDB_MODE_UNKNOWN
	}
}

// GlobalDbFailoverPluginFactory creates GlobalDbFailoverPlugin instances.
type GlobalDbFailoverPluginFactory struct{}

func (f GlobalDbFailoverPluginFactory) GetInstance(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	return NewGlobalDbFailoverPlugin(servicesContainer, props)
}

func (f GlobalDbFailoverPluginFactory) ClearCaches() {}

func NewGlobalDbFailoverPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return GlobalDbFailoverPluginFactory{}
}

// GlobalDbFailoverPlugin implements region-aware failover for Global Aurora Databases.
// It composes with FailoverPlugin for shared infrastructure (telemetry, stale DNS, connection creation)
// but implements its own failover logic with home-region awareness.
type GlobalDbFailoverPlugin struct {
	base                     *FailoverPlugin
	activeHomeFailoverMode   GlobalDbFailoverMode
	inactiveHomeFailoverMode GlobalDbFailoverMode
	homeRegion               string
	rdsUrlType               utils.RdsUrlType
	failoverModeInitialized  bool
}

func NewGlobalDbFailoverPlugin(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
) (*GlobalDbFailoverPlugin, error) {
	base, err := NewFailoverPlugin(servicesContainer, props)
	if err != nil {
		return nil, err
	}
	return &GlobalDbFailoverPlugin{
		base: base,
	}, nil
}

// =============================================================================
// ConnectionPlugin interface — delegated methods
// =============================================================================

func (p *GlobalDbFailoverPlugin) GetPluginCode() string {
	return driver_infrastructure.GDB_FAILOVER_PLUGIN_CODE
}

func (p *GlobalDbFailoverPlugin) GetSubscribedMethods() []string {
	return p.base.GetSubscribedMethods()
}

func (p *GlobalDbFailoverPlugin) InitHostProvider(
	props *utils.RWMap[string, string],
	hostListProviderService driver_infrastructure.HostListProviderService,
	initHostProviderFunc func() error) error {
	return p.base.InitHostProvider(props, hostListProviderService, initHostProviderFunc)
}

func (p *GlobalDbFailoverPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	p.initFailoverMode()

	var conn driver.Conn

	_, internalConnect := props.Get(property_util.INTERNAL_CONNECT_PROPERTY_NAME)
	if !property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.ENABLE_CONNECT_FAILOVER) || internalConnect {
		return p.base.staleDnsHelper.GetVerifiedConnection(hostInfo.Host, isInitialConnection, p.base.hostListProviderService, props, connectFunc)
	}

	var hostInfoWithAvailability *host_info_util.HostInfo
	hosts := utils.FilterSlice(p.base.servicesContainer.GetPluginService().GetHosts(), func(item *host_info_util.HostInfo) bool {
		return item.GetHostAndPort() == hostInfo.GetHostAndPort()
	})
	if len(hosts) != 0 {
		hostInfoWithAvailability = hosts[0]
	}

	if hostInfoWithAvailability.IsNil() || hostInfoWithAvailability.Availability != host_info_util.UNAVAILABLE {
		var err error
		conn, err = p.base.staleDnsHelper.GetVerifiedConnection(hostInfo.Host, isInitialConnection, p.base.hostListProviderService, props, connectFunc)
		if err != nil {
			if !p.base.shouldErrorTriggerConnectionSwitch(err) {
				return nil, err
			}
			p.base.servicesContainer.GetPluginService().SetAvailability(hostInfo.AllAliases, host_info_util.UNAVAILABLE)
			err = p.failover()
			if errors.Is(err, error_util.FailoverSuccessError) {
				conn = p.base.servicesContainer.GetPluginService().GetCurrentConnection()
			} else {
				return nil, err
			}
		}
	} else {
		refreshErr := p.base.servicesContainer.GetPluginService().RefreshHostList(conn)
		if refreshErr != nil {
			return nil, refreshErr
		}
		err := p.failover()
		if errors.Is(err, error_util.FailoverSuccessError) {
			conn = p.base.servicesContainer.GetPluginService().GetCurrentConnection()
		} else {
			return nil, err
		}
	}

	if conn == nil {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("Failover.unableToConnect"))
	}

	if isInitialConnection {
		refreshErr := p.base.servicesContainer.GetPluginService().RefreshHostList(conn)
		if refreshErr != nil {
			return nil, refreshErr
		}
	}

	return conn, nil
}

func (p *GlobalDbFailoverPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return p.base.ForceConnect(hostInfo, props, isInitialConnection, connectFunc)
}

func (p *GlobalDbFailoverPlugin) Execute(
	conn driver.Conn,
	methodName string,
	executeFunc driver_infrastructure.ExecuteFunc,
	methodArgs ...any) (any, any, bool, error) {
	if p.base.canDirectExecute(methodName) {
		return executeFunc()
	}

	wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr := executeFunc()
	var err error
	if wrappedErr != nil {
		err = p.dealWithError(wrappedErr)
	}

	if err != nil {
		return nil, nil, false, err
	}

	return wrappedReturnValue, wrappedReturnValue2, wrappedOk, wrappedErr
}

func (p *GlobalDbFailoverPlugin) AcceptsStrategy(strategy string) bool {
	return p.base.AcceptsStrategy(strategy)
}

func (p *GlobalDbFailoverPlugin) GetHostInfoByStrategy(role host_info_util.HostRole, strategy string, hosts []*host_info_util.HostInfo) (*host_info_util.HostInfo, error) {
	return p.base.GetHostInfoByStrategy(role, strategy, hosts)
}

func (p *GlobalDbFailoverPlugin) GetHostSelectorStrategy(strategy string) (driver_infrastructure.HostSelector, error) {
	return p.base.GetHostSelectorStrategy(strategy)
}

func (p *GlobalDbFailoverPlugin) NotifyConnectionChanged(changes map[driver_infrastructure.HostChangeOptions]bool) driver_infrastructure.OldConnectionSuggestedAction {
	return p.base.NotifyConnectionChanged(changes)
}

func (p *GlobalDbFailoverPlugin) NotifyHostListChanged(changes map[string]map[driver_infrastructure.HostChangeOptions]bool) {
	p.base.NotifyHostListChanged(changes)
}

// =============================================================================
// Global DB failover logic
// =============================================================================

func (p *GlobalDbFailoverPlugin) initFailoverMode() {
	if p.failoverModeInitialized {
		return
	}

	initialHostInfo := p.base.servicesContainer.GetPluginService().GetInitialConnectionHostInfo()
	p.rdsUrlType = utils.IdentifyRdsUrlType(initialHostInfo.Host)

	p.homeRegion = property_util.GetVerifiedWrapperPropertyValue[string](p.base.props, property_util.GDB_FAILOVER_HOME_REGION)
	if p.homeRegion == "" {
		if !p.rdsUrlType.HasRegion {
			slog.Error(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.missingHomeRegion"))
			return
		}
		p.homeRegion = utils.GetRdsRegion(initialHostInfo.Host)
		if p.homeRegion == "" {
			slog.Error(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.missingHomeRegion"))
			return
		}
	}

	slog.Debug(error_util.GetMessage("Failover.parameterValue", "failoverHomeRegion", p.homeRegion))

	activeVal := property_util.GetVerifiedWrapperPropertyValue[string](p.base.props, property_util.GDB_FAILOVER_ACTIVE_HOME_MODE)
	p.activeHomeFailoverMode = gdbFailoverModeFromValue(activeVal)

	inactiveVal := property_util.GetVerifiedWrapperPropertyValue[string](p.base.props, property_util.GDB_FAILOVER_INACTIVE_HOME_MODE)
	p.inactiveHomeFailoverMode = gdbFailoverModeFromValue(inactiveVal)

	if p.activeHomeFailoverMode == GDB_MODE_UNKNOWN {
		if p.rdsUrlType == utils.RDS_WRITER_CLUSTER || p.rdsUrlType == utils.RDS_GLOBAL_WRITER_CLUSTER {
			p.activeHomeFailoverMode = GDB_MODE_STRICT_WRITER
		} else {
			p.activeHomeFailoverMode = GDB_MODE_HOME_READER_OR_WRITER
		}
	}

	if p.inactiveHomeFailoverMode == GDB_MODE_UNKNOWN {
		if p.rdsUrlType == utils.RDS_WRITER_CLUSTER || p.rdsUrlType == utils.RDS_GLOBAL_WRITER_CLUSTER {
			p.inactiveHomeFailoverMode = GDB_MODE_STRICT_WRITER
		} else {
			p.inactiveHomeFailoverMode = GDB_MODE_HOME_READER_OR_WRITER
		}
	}

	slog.Debug(error_util.GetMessage("Failover.parameterValue", "activeHomeFailoverMode", string(p.activeHomeFailoverMode)))
	slog.Debug(error_util.GetMessage("Failover.parameterValue", "inactiveHomeFailoverMode", string(p.inactiveHomeFailoverMode)))

	p.failoverModeInitialized = true
}

func (p *GlobalDbFailoverPlugin) dealWithError(err error) error {
	if err != nil {
		slog.Debug(error_util.GetMessage("Failover.detectedError", err.Error()))
		if !errors.Is(err, p.base.lastErrorDealtWith) && p.base.shouldErrorTriggerConnectionSwitch(err) {
			p.base.InvalidateCurrentConnection()
			currentHost, e := p.base.servicesContainer.GetPluginService().GetCurrentHostInfo()
			if e != nil {
				return e
			}
			p.base.servicesContainer.GetPluginService().SetAvailability(currentHost.Aliases, host_info_util.UNAVAILABLE)
			e = p.failover()
			if e != nil {
				return e
			}
			p.base.lastErrorDealtWith = err
		}
	}
	return err
}

func (p *GlobalDbFailoverPlugin) failover() error {
	failoverStartTime := time.Now()
	failoverEndNano := failoverStartTime.Add(time.Duration(p.base.failoverTimeoutMsSetting) * time.Millisecond)

	parentCtx := p.base.servicesContainer.GetPluginService().GetTelemetryContext()
	telemetryCtx, ctx := p.base.servicesContainer.GetPluginService().GetTelemetryFactory().OpenTelemetryContext(
		telemetry.TELEMETRY_WRITER_FAILOVER, telemetry.NESTED, parentCtx)
	p.base.servicesContainer.GetPluginService().SetTelemetryContext(ctx)

	defer func() {
		slog.Debug(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.failoverElapsed",
			time.Since(failoverStartTime).Milliseconds()))
		telemetryCtx.CloseContext()
		p.base.servicesContainer.GetPluginService().SetTelemetryContext(parentCtx)
		if p.base.telemetryFailoverAdditionalTopTraceSetting {
			postErr := p.base.servicesContainer.GetPluginService().GetTelemetryFactory().PostCopy(telemetryCtx, telemetry.FORCE_TOP_LEVEL)
			if postErr != nil {
				slog.Error(postErr.Error())
			}
		}
	}()

	slog.Info(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.startFailover"))

	// It's expected that this method synchronously returns when topology is stabilized,
	// i.e. when cluster control plane has already chosen a new writer.
	forceRefreshOk, _ := p.base.servicesContainer.GetPluginService().ForceRefreshHostListWithTimeout(true, p.base.failoverTimeoutMsSetting)
	if !forceRefreshOk {
		p.base.failoverWriterTriggeredCounter.Inc(ctx)
		p.base.failoverWriterFailedCounter.Inc(ctx)
		slog.Error(error_util.GetMessage("Failover.unableToRefreshHostList"))
		err := error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToRefreshHostList"))
		telemetryCtx.SetSuccess(false)
		telemetryCtx.SetError(err)
		return err
	}

	updatedHosts := p.base.servicesContainer.GetPluginService().GetAllHosts()
	writerCandidate := host_info_util.GetWriter(updatedHosts)

	if writerCandidate.IsNil() {
		p.base.failoverWriterTriggeredCounter.Inc(ctx)
		p.base.failoverWriterFailedCounter.Inc(ctx)
		message := utils.LogTopology(updatedHosts, error_util.GetMessage("Failover.noWriterHost"))
		slog.Error(message)
		err := error_util.NewFailoverFailedError(message)
		telemetryCtx.SetSuccess(false)
		telemetryCtx.SetError(err)
		return err
	}

	// Check writer region
	writerRegion := utils.GetRdsRegion(writerCandidate.Host)
	isHomeRegion := strings.EqualFold(p.homeRegion, writerRegion)
	slog.Debug(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.isHomeRegion", isHomeRegion))

	var currentFailoverMode GlobalDbFailoverMode
	if isHomeRegion {
		currentFailoverMode = p.activeHomeFailoverMode
	} else {
		currentFailoverMode = p.inactiveHomeFailoverMode
	}
	slog.Debug(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.currentFailoverMode", string(currentFailoverMode)))

	var failoverErr error
	switch currentFailoverMode {
	case GDB_MODE_STRICT_WRITER:
		failoverErr = p.failoverToWriter(writerCandidate, ctx)
	case GDB_MODE_STRICT_HOME_READER:
		failoverErr = p.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.base.servicesContainer.GetPluginService().GetHosts(), func(h *host_info_util.HostInfo) bool {
					return h.Role == host_info_util.READER && strings.EqualFold(utils.GetRdsRegion(h.Host), p.homeRegion)
				})
			},
			host_info_util.READER,
			failoverEndNano,
			ctx)
	case GDB_MODE_STRICT_OUT_OF_HOME_READER:
		failoverErr = p.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.base.servicesContainer.GetPluginService().GetHosts(), func(h *host_info_util.HostInfo) bool {
					return h.Role == host_info_util.READER && !strings.EqualFold(utils.GetRdsRegion(h.Host), p.homeRegion)
				})
			},
			host_info_util.READER,
			failoverEndNano,
			ctx)
	case GDB_MODE_STRICT_ANY_READER:
		failoverErr = p.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.base.servicesContainer.GetPluginService().GetHosts(), func(h *host_info_util.HostInfo) bool {
					return h.Role == host_info_util.READER
				})
			},
			host_info_util.READER,
			failoverEndNano,
			ctx)
	case GDB_MODE_HOME_READER_OR_WRITER:
		failoverErr = p.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.base.servicesContainer.GetPluginService().GetHosts(), func(h *host_info_util.HostInfo) bool {
					return h.Role == host_info_util.WRITER ||
						(h.Role == host_info_util.READER && strings.EqualFold(utils.GetRdsRegion(h.Host), p.homeRegion))
				})
			},
			"", // no role verification
			failoverEndNano,
			ctx)
	case GDB_MODE_OUT_OF_HOME_READER_OR_WRITER:
		failoverErr = p.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.base.servicesContainer.GetPluginService().GetHosts(), func(h *host_info_util.HostInfo) bool {
					return h.Role == host_info_util.WRITER ||
						(h.Role == host_info_util.READER && !strings.EqualFold(utils.GetRdsRegion(h.Host), p.homeRegion))
				})
			},
			"", // no role verification
			failoverEndNano,
			ctx)
	case GDB_MODE_ANY_READER_OR_WRITER:
		failoverErr = p.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return p.base.servicesContainer.GetPluginService().GetHosts()
			},
			"", // no role verification
			failoverEndNano,
			ctx)
	default:
		failoverErr = error_util.NewGenericAwsWrapperError("unsupported failover mode: " + string(currentFailoverMode))
	}

	if failoverErr != nil {
		if errors.Is(failoverErr, error_util.FailoverSuccessError) || errors.Is(failoverErr, error_util.TransactionResolutionUnknownError) {
			telemetryCtx.SetSuccess(true)
			telemetryCtx.SetError(failoverErr)
		} else {
			telemetryCtx.SetSuccess(false)
			telemetryCtx.SetError(failoverErr)
		}
		return failoverErr
	}

	currentHostInfo, err := p.base.servicesContainer.GetPluginService().GetCurrentHostInfo()
	if err != nil {
		return err
	}
	slog.Info(error_util.GetMessage("Failover.establishedConnection", currentHostInfo.String()))
	successErr := p.base.returnFailoverSuccessError()
	telemetryCtx.SetSuccess(true)
	telemetryCtx.SetError(successErr)
	return successErr
}

func (p *GlobalDbFailoverPlugin) failoverToWriter(writerCandidate *host_info_util.HostInfo, ctx context.Context) error {
	p.base.failoverWriterTriggeredCounter.Inc(ctx)

	allowedHosts := p.base.servicesContainer.GetPluginService().GetHosts()
	found := false
	for _, h := range allowedHosts {
		if h.GetHostAndPort() == writerCandidate.GetHostAndPort() {
			found = true
			break
		}
	}
	if !found {
		p.base.failoverWriterFailedCounter.Inc(ctx)
		topologyString := utils.LogTopology(allowedHosts, "")
		message := error_util.GetMessage("Failover.noWriterHost") + " " + writerCandidate.GetUrl() + " " + topologyString
		slog.Error(message)
		return error_util.NewFailoverFailedError(message)
	}

	writerCandidateConn, err := p.base.createConnectionForHost(writerCandidate)
	if err != nil {
		p.base.failoverWriterFailedCounter.Inc(ctx)
		slog.Error(error_util.GetMessage("Failover.errorConnectingToWriter", writerCandidate.Host))
		return error_util.NewFailoverFailedError(error_util.GetMessage("Failover.errorConnectingToWriter", writerCandidate.Host))
	}

	role := p.base.servicesContainer.GetPluginService().GetHostRole(writerCandidateConn)
	if role != host_info_util.WRITER {
		_ = writerCandidateConn.Close()
		p.base.failoverWriterFailedCounter.Inc(ctx)
		message := error_util.GetMessage("Failover.unexpectedReaderRole", writerCandidate.Host, role)
		slog.Error(message)
		return error_util.NewFailoverFailedError(message)
	}

	setErr := p.base.servicesContainer.GetPluginService().SetCurrentConnection(writerCandidateConn, writerCandidate, nil)
	if setErr != nil {
		p.base.failoverWriterFailedCounter.Inc(ctx)
		return setErr
	}

	p.base.failoverWriterSuccessCounter.Inc(ctx)
	return nil
}

func (p *GlobalDbFailoverPlugin) failoverToAllowedHost(
	allowedHostsFunc func() []*host_info_util.HostInfo,
	verifyRole host_info_util.HostRole,
	failoverEnd time.Time,
	ctx context.Context) error {

	p.base.failoverReaderTriggeredCounter.Inc(ctx)

	result, err := p.getAllowedFailoverConnection(allowedHostsFunc, verifyRole, failoverEnd)
	if err != nil {
		p.base.failoverReaderFailedCounter.Inc(ctx)
		slog.Error(error_util.GetMessage("Failover.unableToConnectToReader"))
		return error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToConnectToReader"))
	}

	setErr := p.base.servicesContainer.GetPluginService().SetCurrentConnection(result.Conn, result.HostInfo, nil)
	if setErr != nil {
		if result.Conn != nil {
			_ = result.Conn.Close()
		}
		p.base.failoverReaderFailedCounter.Inc(ctx)
		return setErr
	}

	p.base.failoverReaderSuccessCounter.Inc(ctx)
	return nil
}

func (p *GlobalDbFailoverPlugin) getAllowedFailoverConnection(
	allowedHostsFunc func() []*host_info_util.HostInfo,
	verifyRole host_info_util.HostRole,
	failoverEnd time.Time) (ReaderFailoverResult, error) {

	for time.Now().Before(failoverEnd) {
		// The roles in this list might not be accurate, depending on whether the new topology has become available yet.
		p.base.servicesContainer.GetPluginService().RefreshHostList(nil)
		updatedAllowedHosts := allowedHostsFunc()

		// Make copies with AVAILABLE availability.
		availableHosts := make([]*host_info_util.HostInfo, 0, len(updatedAllowedHosts))
		for _, h := range updatedAllowedHosts {
			copy := h.MakeCopyWithRole(h.Role)
			copy.Availability = host_info_util.AVAILABLE
			availableHosts = append(availableHosts, copy)
		}

		if len(availableHosts) == 0 {
			p.shortDelay()
			continue
		}

		remainingHosts := make([]*host_info_util.HostInfo, len(availableHosts))
		copy(remainingHosts, availableHosts)

		for len(remainingHosts) > 0 && time.Now().Before(failoverEnd) {
			candidateHost, err := p.base.servicesContainer.GetPluginService().GetHostInfoByStrategy(
				verifyRole,
				p.base.failoverReaderHostSelectorStrategySetting,
				remainingHosts)
			if err != nil {
				// Strategy can't get a host according to requested conditions.
				candidateHost = nil
			}

			if candidateHost == nil || candidateHost.IsNil() {
				slog.Debug(utils.LogTopology(remainingHosts,
					error_util.GetMessage("GlobalDbFailoverConnectionPlugin.candidateNull", string(verifyRole))))
				p.shortDelay()
				break
			}

			candidateConn, connErr := p.base.createConnectionForHost(candidateHost)
			if connErr != nil || candidateConn == nil {
				remainingHosts = removeHost(remainingHosts, candidateHost)
				if candidateConn != nil {
					_ = candidateConn.Close()
				}
				continue
			}

			// Since the roles in the host list might not be accurate, we execute a query to check the instance's role.
			if verifyRole != "" {
				role := p.base.servicesContainer.GetPluginService().GetHostRole(candidateConn)
				if role != verifyRole {
					// The role is not as expected, so the connection is not valid.
					remainingHosts = removeHost(remainingHosts, candidateHost)
					_ = candidateConn.Close()
					continue
				}
				updatedHostInfo := candidateHost.MakeCopyWithRole(role)
				return ReaderFailoverResult{Conn: candidateConn, HostInfo: updatedHostInfo}, nil
			}

			return ReaderFailoverResult{Conn: candidateConn, HostInfo: candidateHost}, nil
		}
	}

	return ReaderFailoverResult{}, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.failoverReaderTimeout"))
}

func (p *GlobalDbFailoverPlugin) shortDelay() {
	time.Sleep(100 * time.Millisecond)
}

func removeHost(hosts []*host_info_util.HostInfo, target *host_info_util.HostInfo) []*host_info_util.HostInfo {
	result := make([]*host_info_util.HostInfo, 0, len(hosts))
	for _, h := range hosts {
		if h.GetHostAndPort() != target.GetHostAndPort() {
			result = append(result, h)
		}
	}
	return result
}
