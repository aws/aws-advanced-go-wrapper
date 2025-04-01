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

package utils

import (
	"awssql/host_info_util"
	"context"
	"database/sql/driver"
	"log/slog"
	"strings"
)

func LogTopology(hosts []host_info_util.HostInfo, msgPrefix string) {
	var sb strings.Builder

	if len(hosts) != 0 {
		sb.WriteString("\n")
		for _, host := range hosts {
			sb.WriteString(host.String())
			sb.WriteString("\n")
		}
	} else {
		sb.WriteString("<nil>")
	}
	slog.Info(msgPrefix, "Topology: ", sb.String())
}

func FindHostInTopology(hosts []host_info_util.HostInfo, hostName string) host_info_util.HostInfo {
	for _, host := range hosts {
		if host.Host == hostName {
			return host
		}
	}
	return host_info_util.HostInfo{}
}

// Directly executes query on conn, and returns the first row.
// Returns nil if unable to obtain a row.
func GetFirstRowFromQuery(conn driver.Conn, query string) []driver.Value {
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		// Unable to query, conn does not implement QueryerContext.
		return nil
	}

	rows, err := queryerCtx.QueryContext(context.Background(), query, nil)
	if err != nil {
		// Query failed.
		return nil
	}

	res := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(res)
	rows.Close()
	if err != nil {
		// Gathering row failed.
		return nil
	}
	return res
}

// Directly executes query on conn and converts all possible values in the first row to strings.
// Any values that cannot be converted are returned as "". Returns nil if unable to obtain a row.
func GetFirstRowFromQueryAsString(conn driver.Conn, query string) []string {
	row := GetFirstRowFromQuery(conn, query)
	if row == nil {
		return nil
	}
	res := make([]string, len(row))
	for i := 0; i < len(res); i++ {
		stringAsInt, ok := row[i].([]uint8)
		if ok {
			// Can be cast to string.
			res[i] = string(stringAsInt)
		} else {
			res[i] = ""
		}
	}
	return res
}

func FilterSlice[T any](slice []T, filter func(T) bool) []T {
	var result []T
	for _, v := range slice {
		if filter(v) {
			result = append(result, v)
		}
	}
	return result
}
