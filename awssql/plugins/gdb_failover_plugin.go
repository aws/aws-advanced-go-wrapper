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
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
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

func gdbFailoverModeFromValue(value string) (GlobalDbFailoverMode, error) {
	if value == "" {
		return GDB_MODE_UNKNOWN, nil
	}
	switch strings.ToLower(value) {
	case "strict-writer":
		return GDB_MODE_STRICT_WRITER, nil
	case "strict-home-reader":
		return GDB_MODE_STRICT_HOME_READER, nil
	case "strict-out-of-home-reader":
		return GDB_MODE_STRICT_OUT_OF_HOME_READER, nil
	case "strict-any-reader":
		return GDB_MODE_STRICT_ANY_READER, nil
	case "home-reader-or-writer":
		return GDB_MODE_HOME_READER_OR_WRITER, nil
	case "out-of-home-reader-or-writer":
		return GDB_MODE_OUT_OF_HOME_READER_OR_WRITER, nil
	case "any-reader-or-writer":
		return GDB_MODE_ANY_READER_OR_WRITER, nil
	default:
		return GDB_MODE_UNKNOWN, fmt.Errorf("invalid GlobalDbFailoverMode value: %s", value)
	}
}

// GlobalDbFailoverPluginFactory creates a FailoverPlugin wired with a globalDbFailoverHandler.
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

// NewGlobalDbFailoverPlugin creates a FailoverPlugin with the globalDbFailoverHandler.
func NewGlobalDbFailoverPlugin(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
) (*FailoverPlugin, error) {
	return NewFailoverPlugin(servicesContainer, props, driver_infrastructure.GDB_FAILOVER_PLUGIN_CODE,
		func(p *FailoverPlugin) FailoverHandler { return newGlobalDbFailoverHandler(p) })
}

// globalDbFailoverHandler implements failoverHandler for Global Aurora Database clusters.
type globalDbFailoverHandler struct {
	plugin                   *FailoverPlugin
	activeHomeFailoverMode   GlobalDbFailoverMode
	inactiveHomeFailoverMode GlobalDbFailoverMode
	homeRegion               string
	rdsUrlType               utils.RdsUrlType
	failoverModeInitialized  bool
}

func newGlobalDbFailoverHandler(plugin *FailoverPlugin) *globalDbFailoverHandler {
	return &globalDbFailoverHandler{plugin: plugin}
}

func (h *globalDbFailoverHandler) initFailoverMode() error {
	if h.failoverModeInitialized {
		return nil
	}

	p := h.plugin
	initialHostInfo := p.servicesContainer.GetPluginService().GetInitialConnectionHostInfo()
	h.rdsUrlType = utils.IdentifyRdsUrlType(initialHostInfo.Host)

	h.homeRegion = property_util.GetVerifiedWrapperPropertyValue[string](p.props, property_util.GDB_FAILOVER_HOME_REGION)
	if h.homeRegion == "" {
		if !h.rdsUrlType.HasRegion {
			return errors.New(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.missingHomeRegion"))
		}
		h.homeRegion = utils.GetRdsRegion(initialHostInfo.Host)
		if h.homeRegion == "" {
			return errors.New(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.missingHomeRegion"))
		}
	}

	slog.Debug(error_util.GetMessage("Failover.parameterValue", "failoverHomeRegion", h.homeRegion))

	activeVal := property_util.GetVerifiedWrapperPropertyValue[string](p.props, property_util.GDB_FAILOVER_ACTIVE_HOME_MODE)
	var err error
	h.activeHomeFailoverMode, err = gdbFailoverModeFromValue(activeVal)
	if err != nil {
		return err
	}

	inactiveVal := property_util.GetVerifiedWrapperPropertyValue[string](p.props, property_util.GDB_FAILOVER_INACTIVE_HOME_MODE)
	h.inactiveHomeFailoverMode, err = gdbFailoverModeFromValue(inactiveVal)
	if err != nil {
		return err
	}

	if h.activeHomeFailoverMode == GDB_MODE_UNKNOWN {
		if h.rdsUrlType == utils.RDS_WRITER_CLUSTER || h.rdsUrlType == utils.RDS_GLOBAL_WRITER_CLUSTER {
			h.activeHomeFailoverMode = GDB_MODE_STRICT_WRITER
		} else {
			h.activeHomeFailoverMode = GDB_MODE_HOME_READER_OR_WRITER
		}
	}

	if h.inactiveHomeFailoverMode == GDB_MODE_UNKNOWN {
		if h.rdsUrlType == utils.RDS_WRITER_CLUSTER || h.rdsUrlType == utils.RDS_GLOBAL_WRITER_CLUSTER {
			h.inactiveHomeFailoverMode = GDB_MODE_STRICT_WRITER
		} else {
			h.inactiveHomeFailoverMode = GDB_MODE_HOME_READER_OR_WRITER
		}
	}

	slog.Debug(error_util.GetMessage("Failover.parameterValue", "activeHomeFailoverMode", string(h.activeHomeFailoverMode)))
	slog.Debug(error_util.GetMessage("Failover.parameterValue", "inactiveHomeFailoverMode", string(h.inactiveHomeFailoverMode)))

	h.failoverModeInitialized = true
	return nil
}

func (h *globalDbFailoverHandler) dealWithError(err error) error {
	p := h.plugin
	if err != nil {
		slog.Debug(error_util.GetMessage("Failover.detectedError", err.Error()))
		if !errors.Is(err, p.lastErrorDealtWith) && p.shouldErrorTriggerConnectionSwitch(err) {
			p.InvalidateCurrentConnection()
			currentHost, e := p.servicesContainer.GetPluginService().GetCurrentHostInfo()
			if e != nil {
				return e
			}
			p.servicesContainer.GetPluginService().SetAvailability(currentHost.Aliases, host_info_util.UNAVAILABLE)
			e = h.failover()
			if e != nil {
				return e
			}
			p.lastErrorDealtWith = err
		}
	}
	return err
}

func (h *globalDbFailoverHandler) failover() error {
	p := h.plugin
	failoverStartTime := time.Now()
	failoverEndNano := failoverStartTime.Add(time.Duration(p.failoverTimeoutMsSetting) * time.Millisecond)

	parentCtx := p.servicesContainer.GetPluginService().GetTelemetryContext()
	telemetryCtx, ctx := p.servicesContainer.GetPluginService().GetTelemetryFactory().OpenTelemetryContext(
		telemetry.TELEMETRY_WRITER_FAILOVER, telemetry.NESTED, parentCtx)
	p.servicesContainer.GetPluginService().SetTelemetryContext(ctx)

	defer func() {
		slog.Debug(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.failoverElapsed",
			time.Since(failoverStartTime).Milliseconds()))
		telemetryCtx.CloseContext()
		p.servicesContainer.GetPluginService().SetTelemetryContext(parentCtx)
		if p.telemetryFailoverAdditionalTopTraceSetting {
			postErr := p.servicesContainer.GetPluginService().GetTelemetryFactory().PostCopy(telemetryCtx, telemetry.FORCE_TOP_LEVEL)
			if postErr != nil {
				slog.Error(postErr.Error())
			}
		}
	}()

	slog.Info(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.startFailover"))

	forceRefreshOk, _ := p.servicesContainer.GetPluginService().ForceRefreshHostListWithTimeout(true, p.failoverTimeoutMsSetting)
	if !forceRefreshOk {
		p.failoverWriterTriggeredCounter.Inc(ctx)
		p.failoverWriterFailedCounter.Inc(ctx)
		slog.Error(error_util.GetMessage("Failover.unableToRefreshHostList"))
		err := error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToRefreshHostList"))
		telemetryCtx.SetSuccess(false)
		telemetryCtx.SetError(err)
		return err
	}

	updatedHosts := p.servicesContainer.GetPluginService().GetAllHosts()
	writerCandidate := host_info_util.GetWriter(updatedHosts)

	if writerCandidate.IsNil() {
		p.failoverWriterTriggeredCounter.Inc(ctx)
		p.failoverWriterFailedCounter.Inc(ctx)
		message := utils.LogTopology(updatedHosts, error_util.GetMessage("Failover.noWriterHost"))
		slog.Error(message)
		err := error_util.NewFailoverFailedError(message)
		telemetryCtx.SetSuccess(false)
		telemetryCtx.SetError(err)
		return err
	}

	writerRegion := utils.GetRdsRegion(writerCandidate.Host)
	isHomeRegion := strings.EqualFold(h.homeRegion, writerRegion)
	slog.Debug(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.isHomeRegion", isHomeRegion))

	var currentFailoverMode GlobalDbFailoverMode
	if isHomeRegion {
		currentFailoverMode = h.activeHomeFailoverMode
	} else {
		currentFailoverMode = h.inactiveHomeFailoverMode
	}
	slog.Debug(error_util.GetMessage("GlobalDbFailoverConnectionPlugin.currentFailoverMode", string(currentFailoverMode)))

	var failoverErr error
	switch currentFailoverMode {
	case GDB_MODE_STRICT_WRITER:
		failoverErr = h.failoverToWriter(writerCandidate, ctx)
	case GDB_MODE_STRICT_HOME_READER:
		failoverErr = h.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.servicesContainer.GetPluginService().GetHosts(), func(hi *host_info_util.HostInfo) bool {
					return hi.Role == host_info_util.READER && strings.EqualFold(utils.GetRdsRegion(hi.Host), h.homeRegion)
				})
			},
			host_info_util.READER, failoverEndNano, ctx)
	case GDB_MODE_STRICT_OUT_OF_HOME_READER:
		failoverErr = h.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.servicesContainer.GetPluginService().GetHosts(), func(hi *host_info_util.HostInfo) bool {
					return hi.Role == host_info_util.READER && !strings.EqualFold(utils.GetRdsRegion(hi.Host), h.homeRegion)
				})
			},
			host_info_util.READER, failoverEndNano, ctx)
	case GDB_MODE_STRICT_ANY_READER:
		failoverErr = h.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.servicesContainer.GetPluginService().GetHosts(), func(hi *host_info_util.HostInfo) bool {
					return hi.Role == host_info_util.READER
				})
			},
			host_info_util.READER, failoverEndNano, ctx)
	case GDB_MODE_HOME_READER_OR_WRITER:
		failoverErr = h.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.servicesContainer.GetPluginService().GetHosts(), func(hi *host_info_util.HostInfo) bool {
					return hi.Role == host_info_util.WRITER ||
						(hi.Role == host_info_util.READER && strings.EqualFold(utils.GetRdsRegion(hi.Host), h.homeRegion))
				})
			},
			"", failoverEndNano, ctx)
	case GDB_MODE_OUT_OF_HOME_READER_OR_WRITER:
		failoverErr = h.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return utils.FilterSlice(p.servicesContainer.GetPluginService().GetHosts(), func(hi *host_info_util.HostInfo) bool {
					return hi.Role == host_info_util.WRITER ||
						(hi.Role == host_info_util.READER && !strings.EqualFold(utils.GetRdsRegion(hi.Host), h.homeRegion))
				})
			},
			"", failoverEndNano, ctx)
	case GDB_MODE_ANY_READER_OR_WRITER:
		failoverErr = h.failoverToAllowedHost(
			func() []*host_info_util.HostInfo {
				return p.servicesContainer.GetPluginService().GetHosts()
			},
			"", failoverEndNano, ctx)
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

	currentHostInfo, err := p.servicesContainer.GetPluginService().GetCurrentHostInfo()
	if err != nil {
		return err
	}
	slog.Info(error_util.GetMessage("Failover.establishedConnection", currentHostInfo.String()))
	successErr := p.returnFailoverSuccessError()
	telemetryCtx.SetSuccess(true)
	telemetryCtx.SetError(successErr)
	return successErr
}

func (h *globalDbFailoverHandler) failoverToWriter(writerCandidate *host_info_util.HostInfo, ctx context.Context) error {
	p := h.plugin
	p.failoverWriterTriggeredCounter.Inc(ctx)

	allowedHosts := p.servicesContainer.GetPluginService().GetHosts()
	found := false
	for _, hi := range allowedHosts {
		if hi.GetHostAndPort() == writerCandidate.GetHostAndPort() {
			found = true
			break
		}
	}
	if !found {
		p.failoverWriterFailedCounter.Inc(ctx)
		topologyStr := utils.LogTopology(allowedHosts, "")
		message := error_util.GetMessage("Failover.newWriterNotAllowed", writerCandidate.GetHostAndPort(), topologyStr)
		slog.Error(message)
		return error_util.NewFailoverFailedError(message)
	}

	writerCandidateConn, err := p.createConnectionForHost(writerCandidate)
	if err != nil {
		p.failoverWriterFailedCounter.Inc(ctx)
		slog.Error(error_util.GetMessage("Failover.errorConnectingToWriter", err.Error()))
		return error_util.NewFailoverFailedError(
			error_util.GetMessage("Failover.errorConnectingToWriter", err.Error()))
	}

	role := p.servicesContainer.GetPluginService().GetHostRole(writerCandidateConn)
	if role != host_info_util.WRITER {
		_ = writerCandidateConn.Close()
		p.failoverWriterFailedCounter.Inc(ctx)
		message := error_util.GetMessage("Failover.unexpectedReaderRole", writerCandidate.GetHost(), role)
		slog.Error(message)
		return error_util.NewFailoverFailedError(message)
	}

	err = p.servicesContainer.GetPluginService().SetCurrentConnection(writerCandidateConn, writerCandidate, nil)
	if err != nil {
		_ = writerCandidateConn.Close()
		p.failoverWriterFailedCounter.Inc(ctx)
		return err
	}

	p.failoverWriterSuccessCounter.Inc(ctx)
	return nil
}

func (h *globalDbFailoverHandler) failoverToAllowedHost(
	allowedHostsFunc func() []*host_info_util.HostInfo,
	verifyRole host_info_util.HostRole,
	failoverEnd time.Time,
	ctx context.Context,
) error {
	p := h.plugin
	p.failoverReaderTriggeredCounter.Inc(ctx)

	result, err := h.getAllowedFailoverConnection(allowedHostsFunc, verifyRole, failoverEnd)
	if err != nil {
		p.failoverReaderFailedCounter.Inc(ctx)
		slog.Error(error_util.GetMessage("Failover.unableToConnectToReader"))
		return error_util.NewFailoverFailedError(error_util.GetMessage("Failover.unableToConnectToReader"))
	}

	setErr := p.servicesContainer.GetPluginService().SetCurrentConnection(result.Conn, result.HostInfo, nil)
	if setErr != nil {
		p.failoverReaderFailedCounter.Inc(ctx)
		return setErr
	}

	p.failoverReaderSuccessCounter.Inc(ctx)
	return nil
}

func (h *globalDbFailoverHandler) getAllowedFailoverConnection(
	allowedHostsFunc func() []*host_info_util.HostInfo,
	verifyRole host_info_util.HostRole,
	failoverEnd time.Time,
) (ReaderFailoverResult, error) {
	p := h.plugin

	for ok := true; ok; ok = time.Now().Before(failoverEnd) {
		_ = p.servicesContainer.GetPluginService().RefreshHostList(nil)
		updatedAllowedHosts := allowedHostsFunc()

		if len(updatedAllowedHosts) == 0 {
			h.shortDelay()
			continue
		}

		// Make copies with availability set to AVAILABLE so host selectors don't skip them.
		remainingHosts := make([]*host_info_util.HostInfo, 0, len(updatedAllowedHosts))
		for _, hi := range updatedAllowedHosts {
			copied, err := host_info_util.NewHostInfoBuilder().
				CopyFrom(hi).
				SetAvailability(host_info_util.AVAILABLE).
				Build()
			if err == nil {
				remainingHosts = append(remainingHosts, copied)
			}
		}

		for len(remainingHosts) > 0 && time.Now().Before(failoverEnd) {
			candidateHost, err := p.servicesContainer.GetPluginService().GetHostInfoByStrategy(
				verifyRole, p.failoverReaderHostSelectorStrategySetting, remainingHosts)
			if err != nil || candidateHost.IsNil() {
				slog.Debug(utils.LogTopology(remainingHosts,
					error_util.GetMessage("GlobalDbFailoverConnectionPlugin.candidateNull", verifyRole)))
				h.shortDelay()
				break
			}

			candidateConn, connErr := p.createConnectionForHost(candidateHost)
			if candidateConn == nil || connErr != nil {
				remainingHosts = utils.RemoveFromSlice(remainingHosts, candidateHost, func(a, b *host_info_util.HostInfo) bool {
					return a.GetHostAndPort() == b.GetHostAndPort()
				})
				continue
			}

			// If verifyRole is empty, accept any role.
			if verifyRole == "" {
				return ReaderFailoverResult{Conn: candidateConn, HostInfo: candidateHost}, nil
			}

			role := p.servicesContainer.GetPluginService().GetHostRole(candidateConn)
			if role == verifyRole {
				updatedHostInfo := candidateHost.MakeCopyWithRole(role)
				return ReaderFailoverResult{Conn: candidateConn, HostInfo: updatedHostInfo}, nil
			}

			// Role doesn't match — close and remove.
			remainingHosts = utils.RemoveFromSlice(remainingHosts, candidateHost, func(a, b *host_info_util.HostInfo) bool {
				return a.GetHostAndPort() == b.GetHostAndPort()
			})
			_ = candidateConn.Close()
		}
	}

	return ReaderFailoverResult{}, error_util.NewFailoverFailedError(error_util.GetMessage("Failover.failoverReaderTimeout"))
}

func (h *globalDbFailoverHandler) shortDelay() {
	time.Sleep(100 * time.Millisecond)
}
