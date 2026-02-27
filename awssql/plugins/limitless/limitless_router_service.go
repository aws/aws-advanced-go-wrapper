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
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// forceGetLimitlessRoutersLockMap holds locks for synchronous router fetching.
var forceGetLimitlessRoutersLockMap = utils.NewRWMap[string, *sync.Mutex]()

type LimitlessRouterService interface {
	EstablishConnection(context *LimitlessConnectionContext) error
	StartMonitoring(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], intervalMs int) error
}

type LimitlessRouterServiceImpl struct {
	servicesContainer driver_infrastructure.ServicesContainer
	pluginService     driver_infrastructure.PluginService
	storageService    driver_infrastructure.StorageService
	monitorService    driver_infrastructure.MonitorService
	queryHelper       LimitlessQueryHelper
	props             *utils.RWMap[string, string]
}

func NewLimitlessRouterServiceImpl(
	servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
) *LimitlessRouterServiceImpl {
	return NewLimitlessRouterServiceImplInternal(
		servicesContainer,
		NewLimitlessQueryHelperImpl(servicesContainer.GetPluginService()),
		props,
	)
}

func NewLimitlessRouterServiceImplInternal(
	servicesContainer driver_infrastructure.ServicesContainer,
	queryHelper LimitlessQueryHelper,
	props *utils.RWMap[string, string],
) *LimitlessRouterServiceImpl {
	pluginService := servicesContainer.GetPluginService()
	storageService := servicesContainer.GetStorageService()
	monitorService := servicesContainer.GetMonitorService()

	monitorDisposalTimeMs := property_util.GetExpirationValue(props, property_util.LIMITLESS_MONITORING_DISPOSAL_TIME_MS)

	// Register the storage type for limitless routers
	LimitlessRoutersStorageType.TTL = time.Millisecond * time.Duration(monitorDisposalTimeMs)
	LimitlessRoutersStorageType.Register(storageService)

	// Register the monitor type with the monitor service
	monitorService.RegisterMonitorType(
		LimitlessRouterMonitorType,
		&driver_infrastructure.MonitorSettings{
			ExpirationTimeout: time.Millisecond * time.Duration(monitorDisposalTimeMs),
			InactiveTimeout:   3 * time.Minute,
			ErrorResponses:    map[driver_infrastructure.MonitorErrorResponse]bool{driver_infrastructure.MonitorErrorRecreate: true},
		},
		LimitlessRoutersStorageType.TypeKey, // Produced data type - extends monitor expiration on data access
	)

	return &LimitlessRouterServiceImpl{
		servicesContainer: servicesContainer,
		pluginService:     pluginService,
		storageService:    storageService,
		monitorService:    monitorService,
		queryHelper:       queryHelper,
		props:             props,
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
	context.LimitlessRouters = routerService.getLimitlessRouters(clusterId)

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
				conn, err := context.ConnectFunc(routerService.props)
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
				conn, err := context.ConnectFunc(routerService.props)
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

	conn, err := routerService.pluginService.Connect(selectedRouter, context.Props, context.plugin)
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

func (routerService *LimitlessRouterServiceImpl) getLimitlessRouters(routerCacheKey string) []*host_info_util.HostInfo {
	routers, ok := LimitlessRoutersStorageType.Get(routerService.storageService, routerCacheKey)
	if ok && routers != nil {
		return routers.GetHosts()
	}
	return []*host_info_util.HostInfo{}
}

func (routerService *LimitlessRouterServiceImpl) synchronousGetLimitlessRoutersWithRetry(context *LimitlessConnectionContext) error {
	slog.Debug(error_util.GetMessage("LimitlessRouterServiceImpl.synchronousGetLimitlessRoutersWithRetry"))
	retryCount := -1 // start at -1 since the first try is not a retry.
	maxRetries, err := property_util.GetPositiveIntProperty(context.Props, property_util.LIMITLESS_GET_ROUTER_MAX_RETRIES)
	if err != nil {
		slog.Error(err.Error())
	}
	retryIntervalMs := property_util.GetRefreshRateValue(context.Props, property_util.LIMITLESS_GET_ROUTER_RETRY_INTERVAL_MS)
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
	routerCacheKey, err := routerService.pluginService.GetHostListProvider().GetClusterId()
	if err != nil {
		return err
	}

	lock := forceGetLimitlessRoutersLockMap.ComputeIfAbsent(
		routerCacheKey,
		func() *sync.Mutex {
			return &sync.Mutex{}
		})

	lock.Lock()
	defer lock.Unlock()

	// Fetch and check routers from storage service
	cachedLimitlessRouters := routerService.getLimitlessRouters(routerCacheKey)
	if len(cachedLimitlessRouters) > 0 {
		context.LimitlessRouters = cachedLimitlessRouters
		return nil
	}

	// Open new context connection
	if context.connection == nil {
		conn, err := context.ConnectFunc(routerService.props)
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
		LimitlessRoutersStorageType.Set(routerService.storageService, routerCacheKey, NewLimitlessRouters(newLimitlessRouters))
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
	maxRetries, err := property_util.GetPositiveIntProperty(context.Props, property_util.LIMITLESS_MAX_CONN_RETRIES)
	if err != nil {
		slog.Error(err.Error())
	}
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
					conn, err := context.ConnectFunc(routerService.props)
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
		conn, err := routerService.pluginService.Connect(selectedRouter, context.Props, context.plugin)
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

func (routerService *LimitlessRouterServiceImpl) StartMonitoring(hostInfo *host_info_util.HostInfo, props *utils.RWMap[string, string], intervalMs int) error {
	cacheKey, err := routerService.pluginService.GetHostListProvider().GetClusterId()
	if err != nil {
		slog.Warn(error_util.GetMessage("LimitlessRouterServiceImpl.errorStartingMonitor", err.Error()))
		return err
	}

	// Capture values for the initializer closure
	limitlessRouterCacheKey := cacheKey
	hostInfoCopy := hostInfo
	propsCopy := props
	intervalMsCopy := intervalMs

	_, err = routerService.monitorService.RunIfAbsent(
		LimitlessRouterMonitorType,
		limitlessRouterCacheKey,
		routerService.servicesContainer,
		func(container driver_infrastructure.ServicesContainer) (driver_infrastructure.Monitor, error) {
			return NewLimitlessRouterMonitorImpl(
				container,
				hostInfoCopy,
				limitlessRouterCacheKey,
				propsCopy,
				intervalMsCopy,
			), nil
		},
	)
	if err != nil {
		slog.Warn(error_util.GetMessage("LimitlessRouterServiceImpl.errorStartingMonitor", err.Error()))
		return err
	}

	return nil
}

// ClearCache clears the force get limitless routers lock map.
func ClearCache() {
	forceGetLimitlessRoutersLockMap.Clear()
}
