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

package custom_endpoint

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// CustomEndpointMonitorType is the type descriptor for custom endpoint monitors.
// Used with MonitorService to manage CustomEndpointMonitor instances.
var CustomEndpointMonitorType = &driver_infrastructure.MonitorType{Name: "CustomEndpointMonitor"}

type CustomEndpointMonitor interface {
	driver_infrastructure.Monitor
	HasCustomEndpointInfo() bool
	RequestCustomEndpointInfoUpdate()
}

var customEndpointInfoCache *utils.CacheMap[*CustomEndpointInfo] = utils.NewCache[*CustomEndpointInfo]()

const CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO = time.Minute * 5

type CustomEndpointMonitorImpl struct {
	servicesContainer      driver_infrastructure.ServicesContainer
	pluginService          driver_infrastructure.PluginService
	customEndpointHostInfo *host_info_util.HostInfo
	endpointIdentifier     string
	region                 region_util.Region
	refreshRateMs          time.Duration
	infoChangedCounter     telemetry.TelemetryCounter
	rdsClient              *rds.Client

	// Monitor interface fields
	stop                      atomic.Bool
	state                     atomic.Value // driver_infrastructure.MonitorState
	lastActivityTimestampNano atomic.Int64
	wg                        sync.WaitGroup

	// Refresh control
	refreshRequired    atomic.Bool
	hasConnectionIssue atomic.Bool
	refreshMu          sync.Mutex
	refreshCond        *sync.Cond
}

func NewCustomEndpointMonitorImpl(
	servicesContainer driver_infrastructure.ServicesContainer,
	customEndpointHostInfo *host_info_util.HostInfo,
	endpointIdentifier string,
	region region_util.Region,
	refreshRateMs time.Duration,
	infoChangedCounter telemetry.TelemetryCounter,
	rdsClient *rds.Client,
) *CustomEndpointMonitorImpl {
	monitor := &CustomEndpointMonitorImpl{
		servicesContainer:      servicesContainer,
		pluginService:          servicesContainer.GetPluginService(),
		customEndpointHostInfo: customEndpointHostInfo,
		endpointIdentifier:     endpointIdentifier,
		region:                 region,
		refreshRateMs:          refreshRateMs,
		infoChangedCounter:     infoChangedCounter,
		rdsClient:              rdsClient,
	}
	monitor.refreshCond = sync.NewCond(&monitor.refreshMu)

	return monitor
}

// Start implements Monitor interface - starts the monitoring goroutine.
func (monitor *CustomEndpointMonitorImpl) Start() {
	monitor.state.Store(driver_infrastructure.MonitorStateRunning)
	monitor.lastActivityTimestampNano.Store(time.Now().UnixNano())
	monitor.wg.Add(1)
	go monitor.Monitor()
}

// Stop implements Monitor interface - stops the monitor and waits for cleanup.
func (monitor *CustomEndpointMonitorImpl) Stop() {
	monitor.stop.Store(true)
	// Wake up any sleeping goroutine
	monitor.refreshMu.Lock()
	monitor.refreshCond.Broadcast()
	monitor.refreshMu.Unlock()
	monitor.wg.Wait()
	monitor.Close()
	monitor.state.Store(driver_infrastructure.MonitorStateStopped)
}

// Monitor implements Monitor interface - the main monitoring loop.
func (monitor *CustomEndpointMonitorImpl) Monitor() {
	defer func() {
		slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.stoppedMonitor", monitor.customEndpointHostInfo.Host))
		customEndpointInfoCache.Remove(monitor.getCustomEndpointInfoCacheKey())
		monitor.wg.Done()
	}()

	slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.startingMonitor", monitor.customEndpointHostInfo.Host))

	for !monitor.stop.Load() {
		start := time.Now()
		monitor.lastActivityTimestampNano.Store(time.Now().UnixNano())

		// RDS SDK call
		command := &rds.DescribeDBClusterEndpointsInput{
			DBClusterEndpointIdentifier: &monitor.endpointIdentifier,
			Filters: []types.Filter{
				{
					Name:   aws.String("db-cluster-endpoint-type"),
					Values: []string{"custom"},
				},
			},
		}
		resp, err := monitor.rdsClient.DescribeDBClusterEndpoints(context.TODO(), command)

		// Error checking
		if err != nil {
			slog.Error(error_util.GetMessage("CustomEndpointMonitorImpl.error", err))
			monitor.sleep(monitor.refreshRateMs)
			continue
		} else if resp == nil || resp.DBClusterEndpoints == nil {
			slog.Error(error_util.GetMessage("CustomEndpointMonitorImpl.nilResponse"))
			monitor.sleep(monitor.refreshRateMs)
			continue
		} else if len(resp.DBClusterEndpoints) != 1 {
			var endpointsString string
			for i, endpoint := range resp.DBClusterEndpoints {
				if endpoint.Endpoint != nil && *endpoint.Endpoint != "" {
					if i > 0 {
						endpointsString = endpointsString + ","
					}
					endpointsString = endpointsString + *endpoint.Endpoint
				}
			}
			slog.Warn(error_util.GetMessage("CustomEndpointMonitorImpl.unexpectedNumberOfEndpoints",
				monitor.endpointIdentifier,
				monitor.region,
				len(resp.DBClusterEndpoints),
				endpointsString))
			monitor.sleep(monitor.refreshRateMs)
			continue
		}

		monitor.hasConnectionIssue.Store(false)

		endpointInfo, err := NewCustomEndpointInfo(resp.DBClusterEndpoints[0])
		if err != nil {
			slog.Error(err.Error())
			monitor.sleep(monitor.refreshRateMs)
			continue
		}
		cachedEndpointInfo, ok := customEndpointInfoCache.Get(monitor.getCustomEndpointInfoCacheKey())

		if ok && endpointInfo.Equals(cachedEndpointInfo) {
			elapsedTime := time.Since(start)
			sleepDuration := monitor.refreshRateMs - elapsedTime
			if sleepDuration < 0 {
				sleepDuration = 0
			}
			monitor.sleep(sleepDuration)
			continue
		}

		slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.detectedChangeInCustomEndpointInfo",
			monitor.customEndpointHostInfo.Host, endpointInfo))

		// Custom Endpoint Info has changed. Update set of allowed/blocked hosts
		var allowedAndBlockedHosts *driver_infrastructure.AllowedAndBlockedHosts
		if STATIC_LIST == endpointInfo.memberListType {
			allowedAndBlockedHosts = driver_infrastructure.NewAllowedAndBlockedHosts(endpointInfo.GetStaticMembers(), nil)
		} else {
			allowedAndBlockedHosts = driver_infrastructure.NewAllowedAndBlockedHosts(nil, endpointInfo.GetExcludedMembers())
		}

		driver_infrastructure.AllowedAndBlockedHostsStorageType.Set(monitor.servicesContainer.GetStorageService(), monitor.customEndpointHostInfo.GetUrl(), allowedAndBlockedHosts)

		customEndpointInfoCache.Put(monitor.getCustomEndpointInfoCacheKey(), endpointInfo, CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO)
		monitor.refreshRequired.Store(false)
		monitor.infoChangedCounter.Inc(monitor.pluginService.GetTelemetryContext())

		elapsedTime := time.Since(start)
		sleepDuration := monitor.refreshRateMs - elapsedTime
		if sleepDuration < 0 {
			sleepDuration = 0
		}
		monitor.sleep(sleepDuration)
	}
}

// sleep waits for the specified duration, but can be interrupted by refreshRequired or stop.
func (monitor *CustomEndpointMonitorImpl) sleep(duration time.Duration) {
	if duration <= 0 {
		return
	}

	endTime := time.Now().Add(duration)
	waitDuration := min(500*time.Millisecond, duration)

	monitor.refreshMu.Lock()
	defer monitor.refreshMu.Unlock()

	for !monitor.refreshRequired.Load() && time.Now().Before(endTime) && !monitor.stop.Load() {
		// Use a timer to implement timeout on the condition wait
		timer := time.AfterFunc(waitDuration, func() {
			monitor.refreshMu.Lock()
			monitor.refreshCond.Broadcast()
			monitor.refreshMu.Unlock()
		})
		monitor.refreshCond.Wait()
		timer.Stop()
	}
}

// Close implements Monitor interface - closes resources.
func (monitor *CustomEndpointMonitorImpl) Close() {
	slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.stoppingMonitor", monitor.customEndpointHostInfo.Host))
	customEndpointInfoCache.Remove(monitor.getCustomEndpointInfoCacheKey())
}

// GetLastActivityTimestampNanos implements Monitor interface.
func (monitor *CustomEndpointMonitorImpl) GetLastActivityTimestampNanos() int64 {
	return monitor.lastActivityTimestampNano.Load()
}

// GetState implements Monitor interface.
func (monitor *CustomEndpointMonitorImpl) GetState() driver_infrastructure.MonitorState {
	if state := monitor.state.Load(); state != nil {
		return state.(driver_infrastructure.MonitorState)
	}
	return driver_infrastructure.MonitorStateStopped
}

// CanDispose implements Monitor interface.
func (monitor *CustomEndpointMonitorImpl) CanDispose() bool {
	return true
}

func (monitor *CustomEndpointMonitorImpl) getCustomEndpointInfoCacheKey() string {
	return monitor.customEndpointHostInfo.Host
}

// HasCustomEndpointInfo returns true if custom endpoint info is available.
func (monitor *CustomEndpointMonitorImpl) HasCustomEndpointInfo() bool {
	_, ok := customEndpointInfoCache.Get(monitor.customEndpointHostInfo.Host)
	if !ok && !monitor.refreshRequired.Load() && !monitor.hasConnectionIssue.Load() {
		// There is no custom endpoint info, probably because the cache entry has expired.
		// Wake up the monitor if it is sleeping.
		monitor.RequestCustomEndpointInfoUpdate()
	}
	return ok
}

// RequestCustomEndpointInfoUpdate requests the monitor to refresh custom endpoint info.
func (monitor *CustomEndpointMonitorImpl) RequestCustomEndpointInfoUpdate() {
	if monitor.hasConnectionIssue.Load() {
		// We can't force update since there's an AWS SDK connectivity issue.
		return
	}
	monitor.refreshMu.Lock()
	monitor.refreshRequired.Store(true)
	monitor.refreshCond.Broadcast()
	monitor.refreshMu.Unlock()
}

// ClearCache clears the shared custom endpoint information cache.
func ClearCache() {
	slog.Info(error_util.GetMessage("CustomEndpointMonitorImpl.clearCache"))
	customEndpointInfoCache.Clear()
}

// CloseAllMonitors stops and removes all custom endpoint monitors.
func CloseAllMonitors(monitorService driver_infrastructure.MonitorService) {
	monitorService.StopAndRemoveByType(CustomEndpointMonitorType)
}
