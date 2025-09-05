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
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	mysql_driver "github.com/aws/aws-advanced-go-wrapper/mysql-driver"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
)

func OpenDb(engine DatabaseEngine, dsn string) (*sql.DB, error) {
	switch engine {
	case PG:
		return sql.Open("awssql-pgx", dsn)
	case MYSQL:
		return sql.Open("awssql-mysql", dsn)
	}
	return nil, fmt.Errorf("unknown engine %s", engine)
}

func NewWrapperDriver(engine DatabaseEngine) driver.Driver {
	switch engine {
	case PG:
		return &pgx_driver.PgxDriver{}
	case MYSQL:
		return &mysql_driver.MySQLDriver{}
	}
	return nil
}

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
	switch deployment {
	case AURORA, AURORA_LIMITLESS:
		switch engine {
		case PG:
			return "SELECT aurora_db_instance_identifier() as id", nil
		case MYSQL:
			return "SELECT @@aurora_server_id as id", nil
		default:
			return "", fmt.Errorf("invalid engine: %s", engine)
		}
	case RDS_MULTI_AZ_CLUSTER:
		switch engine {
		case PG:
			return "SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as id FROM rds_tools.show_topology() WHERE id IN" +
				"(SELECT dbi_resource_id FROM rds_tools.dbi_resource_id())", nil
		case MYSQL:
			return "SELECT SUBSTRING_INDEX(endpoint, '.', 1) as id FROM mysql.rds_topology WHERE id=@@server_id", nil
		default:
			return "", fmt.Errorf("invalid engine: %s", engine)
		}
	}
	return "", fmt.Errorf("invalid deployment: %s", deployment)
}

func ExecuteInstanceQueryDB(engine DatabaseEngine, deployment DatabaseEngineDeployment, db *sql.DB) (string, error) {
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

func ExecuteInstanceQuery(engine DatabaseEngine, deployment DatabaseEngineDeployment, conn driver.Conn) (string, error) {
	sql, err := GetInstanceIdSql(engine, deployment)
	if err != nil || sql == "" {
		return "", err
	}
	return GetFirstItemFromQueryAsString(engine, conn, sql)
}

func GetFirstItemFromQueryAsString(engine DatabaseEngine, conn driver.Conn, query string) (string, error) {
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		return "", errors.New("conn does not implement QueryerContext")
	}

	rows, err := queryerCtx.QueryContext(context.TODO(), query, nil)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if len(rows.Columns()) == 0 {
		return "", errors.New("nothing returned from query")
	}

	firstRow := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(firstRow)
	if err != nil {
		return "", err
	}
	if engine == MYSQL {
		stringAsInt, ok := firstRow[0].([]uint8)
		if ok {
			return string(stringAsInt), nil
		}
	} else {
		firstItem, ok := firstRow[0].(string)
		if ok {
			return firstItem, nil
		}
	}
	return "", errors.New("unable to cast result")
}

func GetFirstItemFromQueryAsInt(conn driver.Conn, query string) (int, error) {
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		return -1, errors.New("conn does not implement QueryerContext")
	}

	rows, err := queryerCtx.QueryContext(context.TODO(), query, nil)
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	if len(rows.Columns()) == 0 {
		return -1, errors.New("nothing returned from query")
	}

	firstRow := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(firstRow)
	if err != nil {
		return -1, err
	}
	firstItem, ok := firstRow[0].(int64)
	if ok {
		return int(firstItem), nil
	}
	return -1, errors.New("unable to cast result")
}

func ExecuteInstanceQueryDbWithTimeout(engine DatabaseEngine, deployment DatabaseEngineDeployment, db *sql.DB, seconds int) (string, error) {
	return ExecuteInstanceQueryDbContextWithTimeout(engine, deployment, db, seconds, context.TODO())
}

func ExecuteInstanceQueryDbContextWithTimeout(
	engine DatabaseEngine,
	deployment DatabaseEngineDeployment,
	db *sql.DB,
	seconds int,
	ctx context.Context) (string, error) {
	return ExecuteInstanceQueryContextWithTimeout(engine, deployment, db, seconds, ctx)
}

func ExecuteInstanceQueryContextWithTimeout(
	engine DatabaseEngine,
	deployment DatabaseEngineDeployment,
	dbOrConnOrTx interface{},
	seconds int,
	ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(seconds))
	defer cancel()
	var instanceId string
	query, err := GetInstanceIdSql(engine, deployment)
	if err != nil || query == "" {
		return "", err
	}

	switch v := dbOrConnOrTx.(type) {
	case *sql.DB:
		if e := v.QueryRowContext(ctx, query).Scan(&instanceId); e != nil {
			return "", e
		}
	case *sql.Conn:
		if e := v.QueryRowContext(ctx, query).Scan(&instanceId); e != nil {
			return "", e
		}
	case *sql.Tx:
		if e := v.QueryRowContext(ctx, query).Scan(&instanceId); e != nil {
			return "", e
		}
	default:
		return "", fmt.Errorf("unsupported type: %T, expected *sql.DB, *sql.Conn, or *sql.Tx", dbOrConnOrTx)
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

func ExecuteQuery(engine DatabaseEngine, conn driver.Conn, sql string, timeoutValueSeconds int) (driver.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeoutValueSeconds))
	defer cancel()

	execerCtx, ok := conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("conn does not implement ExecerContext")
	}

	return execerCtx.ExecContext(ctx, sql, nil)
}

func ExecuteQueryDB(engine DatabaseEngine, db *sql.DB, sql string, timeoutValueSeconds int) (*sql.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(timeoutValueSeconds))
	defer cancel()
	return db.QueryContext(ctx, sql)
}

func GetDsn(environment *TestEnvironment, props map[string]string) string {
	return ConstructDsn(environment.Info().Request.Engine, ConfigureProps(environment, props))
}

func ConfigureProps(environment *TestEnvironment, props map[string]string) map[string]string {
	if props["failureDetectionTimeMs"] == "" {
		props["failureDetectionTimeMs"] = "1000"
	}
	if _, ok := props[property_util.USER.Name]; !ok {
		props[property_util.USER.Name] = environment.Info().DatabaseInfo.Username
	}
	if _, ok := props[property_util.HOST.Name]; !ok {
		if slices.Contains(environment.Info().Request.Features, LIMITLESS_DEPLOYMENT) {
			props[property_util.HOST.Name] = strings.Replace(environment.Info().DatabaseInfo.ClusterEndpoint, "cluster-", "shardgrp-", 1)
		} else {
			props[property_util.HOST.Name] = environment.Info().DatabaseInfo.ClusterEndpoint
		}
	}
	if _, ok := props[property_util.DATABASE.Name]; !ok {
		props[property_util.DATABASE.Name] = environment.Info().DatabaseInfo.DefaultDbName
	}
	if _, ok := props[property_util.PASSWORD.Name]; !ok {
		props[property_util.PASSWORD.Name] = environment.Info().DatabaseInfo.Password
	}
	if _, ok := props[property_util.PORT.Name]; !ok {
		props[property_util.PORT.Name] = strconv.Itoa(environment.Info().DatabaseInfo.InstanceEndpointPort)
	}
	return props
}

var requiredProps = []string{
	property_util.USER.Name,
	property_util.PASSWORD.Name,
	property_util.HOST.Name,
	property_util.PORT.Name,
	property_util.DATABASE.Name,
}

func ConstructDsn(engine DatabaseEngine, props map[string]string) (dsn string) {
	switch engine {
	case PG:
		for propKey, propValue := range props {
			dsn = dsn + fmt.Sprintf("%s=%s ", propKey, propValue)
		}
	case MYSQL:
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?",
			props[property_util.USER.Name],
			props[property_util.PASSWORD.Name],
			props[property_util.HOST.Name],
			props[property_util.PORT.Name],
			props[property_util.DATABASE.Name])
		for propKey, propValue := range props {
			if !slices.Contains(requiredProps, propKey) {
				dsn = dsn + fmt.Sprintf("%s=%s&", propKey, propValue)
			}
		}
	}
	return dsn
}
