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
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/property_util"
	"awssql/utils"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var LIMITLESS_ROUTER_MONITOR_CACHE *utils.SlidingExpirationCache[LimitlessRouterMonitor]
var LIMITLESS_ROUTER_CACHE *utils.SlidingExpirationCache[[]*host_info_util.HostInfo]
var LIMITLESS_SYNC_ROUTER_FETCH_LOCK_MAP *utils.SlidingExpirationCache[*sync.Mutex]
var limitlessRouterServiceInitializationMutex sync.Mutex

type LimitlessRouterService interface {
	EstablishConnection(context *LimitlessConnectionContext) error
	StartMonitoring(hostInfo *host_info_util.HostInfo, props map[string]string, intervalMs int) error
}

type LimitlessRouterServiceImpl struct {
	pluginService driver_infrastructure.PluginService
	queryHelper   LimitlessQueryHelper
}

func NewLimitlessRouterServiceImpl(pluginService driver_infrastructure.PluginService) *LimitlessRouterServiceImpl {
	return NewLimitlessRouterServiceImplInternal(pluginService, NewLimitlessQueryHelperImpl(pluginService))
}

func NewLimitlessRouterServiceImplInternal(pluginService driver_infrastructure.PluginService, queryHelper LimitlessQueryHelper) *LimitlessRouterServiceImpl {
	limitlessRouterServiceInitializationMutex.Lock()
	defer limitlessRouterServiceInitializationMutex.Unlock()

	if LIMITLESS_ROUTER_MONITOR_CACHE == nil {
		LIMITLESS_ROUTER_MONITOR_CACHE = utils.NewSlidingExpirationCache[LimitlessRouterMonitor](
			"limitless_router_monitors",
			func(monitor LimitlessRouterMonitor) bool {
				monitor.Close()
				return false
			},
			func(monitor LimitlessRouterMonitor) bool {
				return true
			})
	}

	if LIMITLESS_ROUTER_CACHE == nil {
		LIMITLESS_ROUTER_CACHE = utils.NewSlidingExpirationCache[[]*host_info_util.HostInfo](
			"limitless_routers",
			func(routers []*host_info_util.HostInfo) bool {
				return false
			},
			func(routers []*host_info_util.HostInfo) bool {
				return true
			})
	}

	if LIMITLESS_SYNC_ROUTER_FETCH_LOCK_MAP == nil {
		LIMITLESS_SYNC_ROUTER_FETCH_LOCK_MAP = utils.NewSlidingExpirationCache[*sync.Mutex](
			"limitless_sync_router_fetch_lock_map",
			func(*sync.Mutex) bool {
				return false
			},
			func(mutex *sync.Mutex) bool {
				return true
			})
	}

	return &LimitlessRouterServiceImpl{
		pluginService: pluginService,
		queryHelper:   queryHelper,
	}
}

func (routerService *LimitlessRouterServiceImpl) SetPluginService(pluginService driver_infrastructure.PluginService) {
	routerService.pluginService = pluginService
}

func (routerService *LimitlessRouterServiceImpl) EstablishConnection(context *LimitlessConnectionContext) error {
	// Get Current Routers
	clusterId, err := routerService.pluginService.GetHostListProvider().GetClusterId()
	if err != nil {
		return err
	}
	context.LimitlessRouters = routerService.getLimitlessRouters(clusterId, context.Props)

	// Empty Cache Fallback
	if len(context.LimitlessRouters) < 1 {
		slog.Debug(error_util.GetMessage("LimitlessRouterServiceImpl.limitlessRouterCacheEmpty"))
		if property_util.GetVerifiedWrapperPropertyValue[bool](context.Props, property_util.LIMITLESS_WAIT_FOR_ROUTER_INFO) {
			err := routerService.synchronousGetLimitlessRoutersWithRetry(context)
			if err != nil {
				return err
			}
		} else {
			// Connect right away using provided host info
			if context.GetConnection() == nil {
				conn, err := context.ConnectFunc()
				if err != nil {
					return err
				}
				context.SetConnection(conn)
			}
			return nil
		}
	}

	// Connect if context.host is a router endpoint
	for _, limitlessRouter := range context.LimitlessRouters {
		if limitlessRouter.Equals(&context.Host) {
			if context.GetConnection() == nil {
				conn, err := context.ConnectFunc()
				if err != nil || conn == nil {
					if routerService.pluginService.IsLoginError(err) {
						return err
					}
					return routerService.retryConnectWithLeastLoadedRouters(context)
				}
				context.SetConnection(conn)
			}
			return nil
		}
	}

	// Select Host and Connect
	selectedRouter, err := routerService.pluginService.GetHostInfoByStrategy(
		host_info_util.WRITER,
		driver_infrastructure.SELECTOR_WEIGHTED_RANDOM,
		context.LimitlessRouters)
	if err != nil || selectedRouter == nil {
		return routerService.retryConnectWithLeastLoadedRouters(context)
	}
	slog.Debug(error_util.GetMessage("LimitlessRouterServiceImpl.selectedHost", selectedRouter.Host))

	conn, err := routerService.pluginService.Connect(selectedRouter, context.Props)
	if err != nil || conn == nil {
		selectedRouter.Availability = host_info_util.UNAVAILABLE
		slog.Debug(error_util.GetMessage("LimitlessRouterServiceImpl.failedToConnectToHost", selectedRouter.Host))
		if routerService.pluginService.IsLoginError(err) {
			return err
		}
		return routerService.retryConnectWithLeastLoadedRouters(context)
	}
	context.SetConnection(conn)
	return nil
}

func (routerService *LimitlessRouterServiceImpl) getLimitlessRouters(routerCacheKey string, props map[string]string) []*host_info_util.HostInfo {
	cacheExpirationNano := time.Millisecond * time.Duration(property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.LIMITLESS_ROUTER_CACHE_EXPIRATIONL_TIME_MS))
	routers, ok := LIMITLESS_ROUTER_CACHE.Get(routerCacheKey, cacheExpirationNano)
	if ok {
		return routers
	}
	return []*host_info_util.HostInfo{}
}

func (routerService *LimitlessRouterServiceImpl) synchronousGetLimitlessRoutersWithRetry(context *LimitlessConnectionContext) error {
	slog.Debug(error_util.GetMessage("LimitlessRouterServiceImpl.synchronousGetLimitlessRoutersWithRetry"))
	retryCount := -1 // start at -1 since the first try is not a retry.
	maxRetries := property_util.GetVerifiedWrapperPropertyValue[int](context.Props, property_util.LIMITLESS_GET_ROUTER_MAX_RETRIES)
	retryIntervalMs := property_util.GetVerifiedWrapperPropertyValue[int](context.Props, property_util.LIMITLESS_GET_ROUTER_RETRY_INTERVAL_MS)
	for retryCount < maxRetries {
		err := routerService.synchronousGetLimitlessRouter(context)
		if err != nil {
			slog.Debug(err.Error())
		}
		if len(context.LimitlessRouters) > 0 {
			return nil
		}
		retryCount++
		time.Sleep(time.Duration(retryIntervalMs) * time.Millisecond)
	}
	return errors.New(error_util.GetMessage("LimitlessRouterServiceImpl.noRoutersAvailable"))
}

func (routerService *LimitlessRouterServiceImpl) synchronousGetLimitlessRouter(context *LimitlessConnectionContext) error {
	// Get lock
	routerCacheExpiration :=
		time.Millisecond * time.Duration(property_util.GetVerifiedWrapperPropertyValue[int](context.Props, property_util.LIMITLESS_MONITORING_DISPOSAL_TIME_MS))
	routerCacheKey, err := routerService.pluginService.GetHostListProvider().GetClusterId()
	if err != nil {
		return err
	}
	lock := LIMITLESS_SYNC_ROUTER_FETCH_LOCK_MAP.ComputeIfAbsent(
		routerCacheKey,
		func() *sync.Mutex {
			return &sync.Mutex{}
		},
		routerCacheExpiration)

	lock.Lock()
	defer lock.Unlock()
	// Fetch and check routers from router cache
	cachedLimitlessRouters, ok := LIMITLESS_ROUTER_CACHE.Get(routerCacheKey, routerCacheExpiration)
	if ok && len(cachedLimitlessRouters) > 0 {
		context.LimitlessRouters = cachedLimitlessRouters
		return nil
	}

	// Open new context connection
	if context.connection == nil {
		conn, err := context.ConnectFunc()
		if err != nil {
			if routerService.pluginService.IsLoginError(err) {
				return err
			}
			return err
		}
		context.SetConnection(conn)
	}

	// Fetch/Query limitless routers
	newLimitlessRouters, err := routerService.queryHelper.QueryForLimitlessRouters(context.GetConnection(), context.Host.Port, context.Props)
	if err != nil {
		return err
	} else if len(newLimitlessRouters) < 1 {
		return errors.New(error_util.GetMessage("LimitlessRouterServiceImpl.fetchedEmptyRouterList"))
	} else {
		context.LimitlessRouters = newLimitlessRouters
		LIMITLESS_ROUTER_CACHE.Put(routerCacheKey, newLimitlessRouters, routerCacheExpiration)
		hostSelector, err := routerService.pluginService.GetHostSelectorStrategy(driver_infrastructure.SELECTOR_WEIGHTED_RANDOM)
		if err != nil {
			slog.Warn(err.Error())
		}
		weightedHostSelector, ok := hostSelector.(driver_infrastructure.WeightedHostSelector)
		if ok {
			hostWeightMap := map[string]int{}
			for _, router := range newLimitlessRouters {
				hostWeightMap[router.Host] = router.Weight
			}
			weightedHostSelector.SetHostWeights(hostWeightMap)
		}
		return nil
	}
}

func (routerService *LimitlessRouterServiceImpl) retryConnectWithLeastLoadedRouters(context *LimitlessConnectionContext) error {
	retryCount := 0
	maxRetries := property_util.GetVerifiedWrapperPropertyValue[int](context.Props, property_util.LIMITLESS_MAX_CONN_RETRIES)
	for retryCount < maxRetries {
		// If context limitless routers empty or none are available, fetch routers synchronously
		noAvailableRoutersFunc := func(routers []*host_info_util.HostInfo) bool {
			for _, router := range routers {
				if router.Availability == host_info_util.AVAILABLE {
					return false
				}
			}
			return true
		}

		if len(context.LimitlessRouters) < 1 || noAvailableRoutersFunc(context.LimitlessRouters) {
			err := routerService.synchronousGetLimitlessRoutersWithRetry(context)
			if err != nil {
				return err
			}

			// if routers are still empty or none available, fallback to context.ConnectFunc()
			if len(context.LimitlessRouters) < 1 || noAvailableRoutersFunc(context.LimitlessRouters) {
				slog.Warn(error_util.GetMessage("LimitlessRouterServiceImpl.noRoutersAvailableForRetry"))
				if context.connection != nil {
					return nil
				} else {
					conn, err := context.ConnectFunc()
					if err != nil || conn == nil {
						if routerService.pluginService.IsLoginError(err) {
							return err
						}
						return errors.New(error_util.GetMessage("LimitlessRouterServiceImpl.unableToConnectNoRoutersAvailable", context.Host.Host))
					}
					context.SetConnection(conn)
					return nil
				}
			}
		}

		// Select least loaded router
		selectedRouter, err := routerService.pluginService.GetHostInfoByStrategy(
			host_info_util.WRITER,
			driver_infrastructure.SELECTOR_HIGHEST_WEIGHT,
			context.LimitlessRouters)

		if err != nil || selectedRouter == nil {
			continue
		}

		// Connect to selected router
		conn, err := routerService.pluginService.Connect(selectedRouter, context.Props)
		if err != nil || conn == nil {
			slog.Debug(error_util.GetMessage("LimitlessRouterServiceImpl.failedToConnectToRouter", selectedRouter.Host))
			selectedRouter.Availability = host_info_util.UNAVAILABLE
			if routerService.pluginService.IsLoginError(err) {
				return err
			}
			continue
		}
		context.SetConnection(conn)
		return nil
	}
	return errors.New(error_util.GetMessage("LimitlessRouterServiceImpl.maxConnectRetriesExceeded"))
}

func (routerService *LimitlessRouterServiceImpl) StartMonitoring(hostInfo *host_info_util.HostInfo, props map[string]string, intervalMs int) error {
	cacheKey, err := routerService.pluginService.GetHostListProvider().GetClusterId()

	if err == nil {
		cacheExpirationNano := time.Millisecond * time.Duration(property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.LIMITLESS_MONITORING_DISPOSAL_TIME_MS))

		LIMITLESS_ROUTER_MONITOR_CACHE.ComputeIfAbsent(
			cacheKey,
			func() LimitlessRouterMonitor {
				return NewLimitlessRouterMonitorImpl(routerService.pluginService, hostInfo, LIMITLESS_ROUTER_CACHE, cacheKey, intervalMs, props)
			},
			cacheExpirationNano)
	} else {
		slog.Warn(error_util.GetMessage("LimitlessRouterServiceImpl.errorStartingMonitor", err.Error()))
		return err
	}
	return nil
}
