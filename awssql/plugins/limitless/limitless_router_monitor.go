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

package limitless

import (
	"database/sql/driver"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// LimitlessRouterMonitorType is the type descriptor for limitless router monitors.
// Used with MonitorService to manage LimitlessRouterMonitor instances.
var LimitlessRouterMonitorType = &driver_infrastructure.MonitorType{Name: "LimitlessRouterMonitor"}

const MONITORING_PROPERTY_PREFIX = "limitless-router-monitor-"

type LimitlessRouterMonitor interface {
	driver_infrastructure.Monitor
}

type LimitlessRouterMonitorImpl struct {
	servicesContainer       driver_infrastructure.ServicesContainer
	hostInfo                *host_info_util.HostInfo
	limitlessRouterCacheKey string
	intervalMs              int
	pluginService           driver_infrastructure.PluginService
	props                   *utils.RWMap[string, string]
	queryHelper             LimitlessQueryHelper
	monitoringConn          driver.Conn

	// Monitor interface fields
	stop                      atomic.Bool
	state                     atomic.Value // driver_infrastructure.MonitorState
	lastActivityTimestampNano atomic.Int64
	wg                        sync.WaitGroup
}

func NewLimitlessRouterMonitorImpl(
	servicesContainer driver_infrastructure.ServicesContainer,
	hostInfo *host_info_util.HostInfo,
	limitlessRouterCacheKey string,
	props *utils.RWMap[string, string],
	intervalMs int,
) *LimitlessRouterMonitorImpl {
	// Copy and process monitoring properties
	copyProps := props.GetAllEntries()
	monitoringProps := utils.NewRWMapFromMap(copyProps)
	for propKey, propValue := range copyProps {
		if trimmed, found := strings.CutPrefix(propKey, MONITORING_PROPERTY_PREFIX); found {
			monitoringProps.Put(trimmed, propValue)
			monitoringProps.Remove(propKey)
		}
	}
	property_util.LIMITLESS_WAIT_FOR_ROUTER_INFO.Set(monitoringProps, "false")

	pluginService := servicesContainer.GetPluginService()

	monitor := &LimitlessRouterMonitorImpl{
		servicesContainer:       servicesContainer,
		hostInfo:                hostInfo,
		limitlessRouterCacheKey: limitlessRouterCacheKey,
		intervalMs:              intervalMs,
		pluginService:           pluginService,
		props:                   monitoringProps,
		queryHelper:             NewLimitlessQueryHelperImpl(pluginService),
	}

	return monitor
}

// Start implements Monitor interface - starts the monitoring goroutine.
func (monitor *LimitlessRouterMonitorImpl) Start() {
	monitor.state.Store(driver_infrastructure.MonitorStateRunning)
	monitor.lastActivityTimestampNano.Store(time.Now().UnixNano())
	monitor.wg.Add(1)
	go monitor.Monitor()
}

// Stop implements Monitor interface - stops the monitor and waits for cleanup.
func (monitor *LimitlessRouterMonitorImpl) Stop() {
	monitor.stop.Store(true)
	monitor.wg.Wait()
	monitor.Close()
	monitor.state.Store(driver_infrastructure.MonitorStateStopped)
}

// Monitor implements Monitor interface - the main monitoring loop.
func (monitor *LimitlessRouterMonitorImpl) Monitor() {
	defer func() {
		monitor.stop.Store(true)
		if monitor.monitoringConn != nil {
			_ = monitor.monitoringConn.Close()
			monitor.monitoringConn = nil
		}
		monitor.wg.Done()
	}()

	slog.Debug(error_util.GetMessage("LimitlessRouterMonitorImpl.startMonitoring", monitor.hostInfo.Host))

	for !monitor.stop.Load() {
		monitor.lastActivityTimestampNano.Store(time.Now().UnixNano())

		openConnErr := monitor.openConnection()
		if openConnErr != nil || monitor.monitoringConn == nil {
			time.Sleep(time.Duration(monitor.intervalMs) * time.Millisecond)
			continue
		}

		newLimitlessRouters, err := monitor.queryHelper.QueryForLimitlessRouters(monitor.monitoringConn, monitor.hostInfo.Port, monitor.props)

		if err != nil {
			slog.Warn(err.Error())
			break
		}
		if len(newLimitlessRouters) < 1 {
			slog.Warn("LimitlessRouterMonitorImpl.noRoutersFetched")
			break
		}

		// Store routers in StorageService
		storageService := monitor.servicesContainer.GetStorageService()
		LimitlessRoutersStorageType.Set(storageService, monitor.limitlessRouterCacheKey, NewLimitlessRouters(newLimitlessRouters))

		// Update host selector weights
		hostSelector, err := monitor.pluginService.GetHostSelectorStrategy(driver_infrastructure.SELECTOR_WEIGHTED_RANDOM)
		if err != nil {
			slog.Warn(err.Error())
			slog.Warn(error_util.GetMessage("LimitlessRouterMonitorImpl.hostSelectorStrategyNotFound"))
		} else if hostSelector == nil {
			slog.Warn(error_util.GetMessage("LimitlessRouterMonitorImpl.hostSelectorStrategyNotFound"))
		} else {
			weightedHostSelector, ok := hostSelector.(driver_infrastructure.WeightedHostSelector)
			if ok {
				hostWeightMap := map[string]int{}
				for _, router := range newLimitlessRouters {
					hostWeightMap[router.Host] = router.Weight
				}
				weightedHostSelector.SetHostWeights(hostWeightMap)
			} else {
				slog.Warn(error_util.GetMessage("LimitlessRouterMonitorImpl.unableToCastHostSelector"))
			}
		}
		slog.Debug(utils.LogTopology(newLimitlessRouters, "LimitlessRouterMonitorImpl"))

		time.Sleep(time.Duration(monitor.intervalMs) * time.Millisecond)
	}
	slog.Debug(error_util.GetMessage("LimitlessRouterMonitorImpl.stopMonitoring", monitor.hostInfo.Host))
}

// Close implements Monitor interface - closes resources.
func (monitor *LimitlessRouterMonitorImpl) Close() {
	slog.Debug(error_util.GetMessage("LimitlessRouterMonitorImpl.closeMonitoring", monitor.hostInfo.Host))
	if monitor.monitoringConn != nil {
		_ = monitor.monitoringConn.Close()
		monitor.monitoringConn = nil
	}
}

// GetLastActivityTimestampNanos implements Monitor interface.
func (monitor *LimitlessRouterMonitorImpl) GetLastActivityTimestampNanos() int64 {
	return monitor.lastActivityTimestampNano.Load()
}

// GetState implements Monitor interface.
func (monitor *LimitlessRouterMonitorImpl) GetState() driver_infrastructure.MonitorState {
	if state := monitor.state.Load(); state != nil {
		return state.(driver_infrastructure.MonitorState)
	}
	return driver_infrastructure.MonitorStateStopped
}

// CanDispose implements Monitor interface.
func (monitor *LimitlessRouterMonitorImpl) CanDispose() bool {
	return true
}

func (monitor *LimitlessRouterMonitorImpl) openConnection() error {
	if monitor.monitoringConn == nil {
		// open a new connection
		slog.Info(error_util.GetMessage("LimitlessRouterMonitorImpl.openingConnection", monitor.hostInfo.Host))
		newConn, err := monitor.pluginService.ForceConnect(monitor.hostInfo, monitor.props)
		if err != nil {
			if newConn != nil {
				_ = newConn.Close()
			}
			return err
		}

		monitor.monitoringConn = newConn
		slog.Info(error_util.GetMessage("LimitlessRouterMonitorImpl.openedConnection", monitor.hostInfo.Host))
	}
	return nil
}

// CloseAllMonitors stops and removes all limitless router monitors.
func CloseAllMonitors(monitorService driver_infrastructure.MonitorService) {
	monitorService.StopAndRemoveByType(LimitlessRouterMonitorType)
}
