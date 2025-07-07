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
	"maps"
	"sync/atomic"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type LimitlessRouterMonitor interface {
	Close()
}

type LimitlessRouterMonitorImpl struct {
	hostInfo       *host_info_util.HostInfo
	routerCache    *utils.SlidingExpirationCache[[]*host_info_util.HostInfo]
	routerCacheKey string
	intervalMs     int
	pluginService  driver_infrastructure.PluginService
	props          map[string]string
	queryHelper    LimitlessQueryHelper
	monitoringConn driver.Conn
	stopped        atomic.Bool
}

func NewLimitlessRouterMonitorImpl(
	pluginService driver_infrastructure.PluginService,
	hostInfo *host_info_util.HostInfo,
	routerCache *utils.SlidingExpirationCache[[]*host_info_util.HostInfo],
	routerCacheKey string,
	intervalMs int,
	props map[string]string) *LimitlessRouterMonitorImpl {
	monitor := &LimitlessRouterMonitorImpl{
		queryHelper:    NewLimitlessQueryHelperImpl(pluginService),
		pluginService:  pluginService,
		hostInfo:       hostInfo,
		routerCache:    routerCache,
		routerCacheKey: routerCacheKey,
		intervalMs:     intervalMs,
		props:          maps.Clone(props),
	}

	property_util.LIMITLESS_WAIT_FOR_ROUTER_INFO.Set(monitor.props, "false")
	go monitor.run()

	return monitor
}

func (monitor *LimitlessRouterMonitorImpl) Close() {
	slog.Debug(error_util.GetMessage("LimitlessRouterMonitorImpl.closeMonitoring", monitor.hostInfo.Host))
	monitor.stopped.Store(true)
	if monitor.monitoringConn != nil {
		_ = monitor.monitoringConn.Close()
		monitor.monitoringConn = nil
	}
}

func (monitor *LimitlessRouterMonitorImpl) run() {
	defer func() {
		monitor.stopped.Store(true)
		if monitor.monitoringConn != nil {
			_ = monitor.monitoringConn.Close()
			monitor.monitoringConn = nil
		}
	}()

	slog.Debug(error_util.GetMessage("LimitlessRouterMonitorImpl.startMonitoring", monitor.hostInfo.Host))

	for !monitor.stopped.Load() {
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

		routerCacheExpiration :=
			time.Millisecond * time.Duration(property_util.GetExpirationValue(monitor.props, property_util.LIMITLESS_ROUTER_CACHE_EXPIRATION_TIME_MS))
		monitor.routerCache.Put(
			monitor.routerCacheKey,
			newLimitlessRouters,
			routerCacheExpiration,
		)
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

func (monitor *LimitlessRouterMonitorImpl) openConnection() error {
	if monitor.monitoringConn == nil {
		// open a new connection
		slog.Info(error_util.GetMessage("LimitlessRouterMonitorImpl.openingConnection", monitor.hostInfo.Host))
		newConn, err := monitor.pluginService.ForceConnect(monitor.hostInfo, utils.CreateMapCopy(monitor.props))
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
