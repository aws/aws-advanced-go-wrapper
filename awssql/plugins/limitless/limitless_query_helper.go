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
	"context"
	"database/sql/driver"
	"errors"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"log/slog"
	"math"
	"time"
)

type LimitlessQueryHelper interface {
	QueryForLimitlessRouters(conn driver.Conn, hostPortToMap int, props map[string]string) (hostInfoList []*host_info_util.HostInfo, err error)
}

type LimitlessQueryHelperImpl struct {
	pluginService driver_infrastructure.PluginService
}

func NewLimitlessQueryHelperImpl(pluginService driver_infrastructure.PluginService) *LimitlessQueryHelperImpl {
	return &LimitlessQueryHelperImpl{
		pluginService: pluginService,
	}
}

func (queryHelper *LimitlessQueryHelperImpl) QueryForLimitlessRouters(
	conn driver.Conn,
	hostPortToMap int,
	props map[string]string) (hostInfoList []*host_info_util.HostInfo, err error) {
	dialect, isDialectLimitless := queryHelper.pluginService.GetDialect().(driver_infrastructure.AuroraLimitlessDialect)
	if !isDialectLimitless {
		return nil, errors.New(error_util.GetMessage("LimitlessQueryHelperImpl.invalidDatabaseDialect", dialect))
	}

	query := dialect.GetLimitlessRouterEndpointQuery()

	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
	}

	timeout := time.Duration(property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.LIMITLESS_ROUTER_QUERY_TIMEOUT_MS)) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rows, err := queryerCtx.QueryContext(ctx, query, nil)
	if err != nil {
		// Query failed.
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	return mapLimitlessQueryRowsToHostInfoList(rows, hostPortToMap), nil
}

func mapLimitlessQueryRowsToHostInfoList(rows driver.Rows, hostPortToMap int) []*host_info_util.HostInfo {
	hosts := []*host_info_util.HostInfo{}

	if rows == nil {
		slog.Warn(error_util.GetMessage("LimitlessQueryHelperImpl.invalidQuery"))
		return hosts
	}

	row := make([]driver.Value, len(rows.Columns()))
	err := rows.Next(row)
	for err == nil && len(row) > 1 {
		hostName, hostNameOk := row[0].(string)
		load, loadOk := row[1].(float64)

		if !hostNameOk || hostName == "" {
			slog.Warn(error_util.GetMessage("LimitlessQueryHelperImpl.unableToFetchRouterName"))
		} else if !loadOk {
			slog.Warn(error_util.GetMessage("LimitlessQueryHelperImpl.unableToFetchFetchingRouterLoad"))
		} else {
			weight := mapLimitlessRouterLoadToHostInfoWeight(hostName, load)

			hostInfoBuilder := host_info_util.NewHostInfoBuilder()
			hostInfoBuilder.
				SetHost(hostName).
				SetPort(hostPortToMap).
				SetWeight(weight).
				SetRole(host_info_util.WRITER).
				SetAvailability(host_info_util.AVAILABLE).
				SetHostId(hostName).
				SetLastUpdateTime(time.Now())

			host, _ := hostInfoBuilder.Build()
			hosts = append(hosts, host)
		}
		err = rows.Next(row)
	}

	return hosts
}

func mapLimitlessRouterLoadToHostInfoWeight(hostName string, routerLoad float64) int {
	weight := int(math.Round(10 - routerLoad*10))
	if weight < 1 || weight > 10 {
		weight = 1
		slog.Warn(error_util.GetMessage("LimitlessQueryHelperImpl.invalidRouterLoad", routerLoad, hostName))
	}
	return weight
}
