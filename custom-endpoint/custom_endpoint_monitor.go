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

type CustomEndpointMonitor interface {
	ShouldDispose() bool
	Close()
	HasCustomEndpointInfo() bool
}

var customEndpointInfoCache *utils.CacheMap[*CustomEndpointInfo] = utils.NewCache[*CustomEndpointInfo]()

const CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO = time.Minute * 5

type CustomEndpointMonitorImpl struct {
	pluginService          driver_infrastructure.PluginService
	customEndpointHostInfo *host_info_util.HostInfo
	endpointIdentifier     string
	region                 region_util.Region
	refreshRateMs          time.Duration
	infoChangedCounter     telemetry.TelemetryCounter
	rdsClient              *rds.Client
	stop                   atomic.Bool
}

func NewCustomEndpointMonitorImpl(
	pluginService driver_infrastructure.PluginService,
	customEndpointHostInfo *host_info_util.HostInfo,
	endpointIdentifier string,
	region region_util.Region,
	refreshRateMs time.Duration,
	rdsClient *rds.Client) *CustomEndpointMonitorImpl {
	monitor := &CustomEndpointMonitorImpl{
		pluginService:          pluginService,
		customEndpointHostInfo: customEndpointHostInfo,
		endpointIdentifier:     endpointIdentifier,
		region:                 region,
		refreshRateMs:          refreshRateMs,
		rdsClient:              rdsClient,
	}

	go monitor.run()

	return monitor
}

func (monitor *CustomEndpointMonitorImpl) run() {
	defer func() {
		slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.stoppedMonitor", monitor.customEndpointHostInfo.Host))
		customEndpointInfoCache.Remove(monitor.getCustomEndpointInfoCacheKey())
	}()

	for !monitor.stop.Load() {
		start := time.Now()

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
			continue
		} else if resp == nil || resp.DBClusterEndpoints == nil {
			slog.Error(error_util.GetMessage("CustomEndpointMonitorImpl.nilResponse"))
			continue
		} else if len(resp.DBClusterEndpoints) != 1 {
			var endpointsString string
			for i, endpoint := range resp.DBClusterEndpoints {
				if i > 0 {
					endpointsString = endpointsString + ","
				}
				endpointsString = endpointsString + *endpoint.Endpoint
			}
			slog.Warn(error_util.GetMessage("CustomEndpointMonitorImpl.unexpectedNumberOfEndpoints",
				monitor.endpointIdentifier,
				monitor.region,
				len(resp.DBClusterEndpoints),
				endpointsString))
			time.Sleep(monitor.refreshRateMs)
			continue
		}

		endpointInfo := NewCustomEndpointInfo(resp.DBClusterEndpoints[0])
		cachedEndpointInfo, ok := customEndpointInfoCache.Get(monitor.getCustomEndpointInfoCacheKey())

		if ok && endpointInfo.Equals(cachedEndpointInfo) {
			elapsedTime := time.Since(start)
			sleepDuration := monitor.refreshRateMs - elapsedTime
			if sleepDuration < 0 {
				sleepDuration = 0
			}
			time.Sleep(sleepDuration)
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

		monitor.pluginService.SetAllowedAndBlockedHosts(allowedAndBlockedHosts)

		customEndpointInfoCache.Put(monitor.customEndpointHostInfo.GetHost(), endpointInfo, CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO)

		elapsedTime := time.Since(start)
		sleepDuration := monitor.refreshRateMs - elapsedTime
		if sleepDuration < 0 {
			sleepDuration = 0
		}
		time.Sleep(sleepDuration)
	}
}

func (monitor *CustomEndpointMonitorImpl) getCustomEndpointInfoCacheKey() string {
	return monitor.customEndpointHostInfo.Host
}

func (monitor *CustomEndpointMonitorImpl) ShouldDispose() bool {
	return true
}

func (monitor *CustomEndpointMonitorImpl) HasCustomEndpointInfo() bool {
	_, ok := customEndpointInfoCache.Get(monitor.customEndpointHostInfo.Host)
	return ok
}

func (monitor *CustomEndpointMonitorImpl) Close() {
	slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.stoppingMonitor", monitor.customEndpointHostInfo.Host))
	monitor.stop.Store(true)
}

func ClearCache() {
	slog.Info(error_util.GetMessage("CustomEndpointMonitorImpl.clearCache"))
	customEndpointInfoCache.Clear()
}
