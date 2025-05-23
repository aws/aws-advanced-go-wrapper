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
	"awssql/error_util"
	"awssql/host_info_util"
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

func LogTopology(hosts []*host_info_util.HostInfo, msgPrefix string) string {
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

	return fmt.Sprintf("%s\n %s%s", msgPrefix, "Topology: ", sb.String())
}

func FindHostInTopology(hosts []*host_info_util.HostInfo, hostNames ...string) *host_info_util.HostInfo {
	for _, host := range hosts {
		for _, hostName := range hostNames {
			if host.Host == hostName {
				return host
			}
		}
	}
	return nil
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
	if rows != nil {
		defer rows.Close()
	}

	res := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(res)
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

func ConvertDriverValueToString(value driver.Value) (string, bool) {
	valueAsString, ok := value.(string)
	if !ok {
		valueAsInt, ok := value.([]uint8)
		if ok {
			valueAsString = string(valueAsInt)
		}
	}
	return valueAsString, ok
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

func SliceAndMapHaveCommonElement[T comparable, V any](sliceA []T, mapOfKeysAndValues map[T]V) bool {
	for item := range mapOfKeysAndValues {
		if slices.Contains(sliceA, item) {
			return true
		}
	}
	return false
}

func AllKeys[T comparable, V any](mapOfKeysAndValues map[T]V) []T {
	keys := make([]T, len(mapOfKeysAndValues))
	i := 0
	for key := range mapOfKeysAndValues {
		keys[i] = key
		i++
	}
	return keys
}

func IsConnectionLost(conn driver.Conn) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connectionPinger, ok := conn.(driver.Pinger)
	if ok {
		err := connectionPinger.Ping(ctx)
		if err != nil {
			// Unable to ping connection in 5 seconds, return that connection is lost.
			return true
		}
	}
	// Unable to confirm that connection is lost.
	return false
}

func IndexOf[T any](slice []T, item T, compareFunc func(T, T) bool) int {
	for i, v := range slice {
		if compareFunc(v, item) {
			return i
		}
	}
	return -1
}

func RemoveFromSlice[T any](slice []T, item T, compareFunc func(T, T) bool) []T {
	index := IndexOf[T](
		slice,
		item,
		compareFunc)
	if index == -1 {
		return slice
	}
	return append(slice[:index], slice[index+1:]...)
}

func LengthOfSyncMap(syncMap *sync.Map) int {
	if syncMap == nil {
		return 0
	}
	var i int
	syncMap.Range(func(key, value interface{}) bool {
		i++
		return true
	})
	return i
}

func CreateMapCopy[K comparable, V any](mapToCopy map[K]V) map[K]V {
	mapCopy := make(map[K]V, len(mapToCopy))
	for key, value := range mapToCopy {
		mapCopy[key] = value
	}
	return mapCopy
}

func Rollback(conn driver.Conn, currentTx driver.Tx) {
	if currentTx != nil {
		err := currentTx.Rollback()
		if err != nil {
			slog.Info(error_util.GetMessage("Utils.rollbackError", err.Error()))
		}
		return
	}

	execerContext, ok := conn.(driver.ExecerContext)
	if ok {
		_, err := execerContext.ExecContext(context.TODO(), "rollback", nil)
		if err != nil {
			slog.Info(error_util.GetMessage("Utils.rollbackError", err.Error()))
		}
	}
}

func GetStructName(v any) string {
	return strings.ReplaceAll(fmt.Sprintf("%T", v), "*", "")
}
