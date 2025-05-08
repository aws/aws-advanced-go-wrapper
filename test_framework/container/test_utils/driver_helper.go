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

package test_utils

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"time"
)

func GetSleepSql(engine DatabaseEngine, seconds int) string {
	switch engine {
	case PG:
		return fmt.Sprintf("select pg_sleep(%d)", seconds)
	case MYSQL:
		return fmt.Sprintf("select sleep(%d)", seconds)
	}
	return ""
}

func GetInstanceIdSql(engine DatabaseEngine, deployment DatabaseEngineDeployment) (string, error) {
	// TODO: deployment = RDS_MULTI_AZ_CLUSTER.
	if deployment == AURORA {
		switch engine {
		case PG:
			return "SELECT aurora_db_instance_identifier() as id", nil
		case MYSQL:
			return "SELECT @@aurora_server_id as id", nil
		default:
			return "", fmt.Errorf("Invalid engine: %s.", engine)
		}
	}
	return "", fmt.Errorf("Invalid deployment: %s.", deployment)
}

func ExecuteInstanceQuery(engine DatabaseEngine, deployment DatabaseEngineDeployment, db *sql.DB) (string, error) {
	var instanceId string
	sql, err := GetInstanceIdSql(engine, deployment)
	if err != nil || sql == "" {
		return "", err
	}
	if e := db.QueryRow(sql).Scan(&instanceId); e != nil {
		return "", e
	}
	return instanceId, nil
}

func ExecuteInstanceQueryWithTimeout(engine DatabaseEngine, deployment DatabaseEngineDeployment, db *sql.DB, seconds int) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(seconds))
	defer cancel()
	var instanceId string
	sql, err := GetInstanceIdSql(engine, deployment)
	if err != nil || sql == "" {
		return "", err
	}
	if e := db.QueryRowContext(ctx, sql).Scan(&instanceId); e != nil {
		return "", e
	}
	return instanceId, nil
}

func ExecuteInstanceQueryWithSleep(engine DatabaseEngine, deployment DatabaseEngineDeployment, db *sql.DB) (instanceId string, err error) {
	sql1 := GetSleepSql(engine, 10)
	sql2, err := GetInstanceIdSql(engine, deployment)
	if err != nil {
		return
	}

	_, err = db.Query(sql1)
	if err != nil {
		return "", err
	}
	if err := db.QueryRow(sql2).Scan(&instanceId); err != nil {
		return "", err
	}
	return instanceId, nil
}

func ExecuteQuery(engine DatabaseEngine, db *sql.DB, sql string, timeoutValueSeconds ...int) (*sql.Rows, error) {
	var ctx context.Context
	if len(timeoutValueSeconds) > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*time.Duration(timeoutValueSeconds[0]))
		defer cancel()
	} else {
		ctx = context.TODO()
	}
	return db.QueryContext(ctx, sql)
}

func GetDsn(environment *TestEnvironment, props map[string]string) string {
	return ConstructDsn(environment.Engine(), ConfigureProps(environment, props))
}

func ConfigureProps(environment *TestEnvironment, props map[string]string) map[string]string {
	if props["failureDetectionTimeMs"] == "" {
		props["failureDetectionTimeMs"] = "1000"
	}
	if _, ok := props["user"]; !ok {
		props["user"] = environment.User()
	}
	if _, ok := props["host"]; !ok {
		props["host"] = environment.ClusterEndpoint()
	}
	if _, ok := props["dbname"]; !ok {
		props["dbname"] = environment.DefaultDbName()
	}
	if _, ok := props["password"]; !ok {
		props["password"] = environment.Password()
	}
	if _, ok := props["port"]; !ok {
		props["port"] = strconv.Itoa(environment.InstanceEndpointPort())
	}
	return props
}

var requiredProps = []string{"user", "password", "host", "port", "dbname"}

func ConstructDsn(engine DatabaseEngine, props map[string]string) (dsn string) {
	switch engine {
	case PG:
		for propKey, propValue := range props {
			dsn = dsn + fmt.Sprintf("%s=%s ", propKey, propValue)
		}
	case MYSQL:
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?", props["user"], props["password"], props["host"], props["port"], props["dbname"])
		for propKey, propValue := range props {
			if !slices.Contains(requiredProps, propKey) {
				dsn = dsn + fmt.Sprintf("%s=%s&", propKey, propValue)
			}
		}
	}
	return dsn
}
