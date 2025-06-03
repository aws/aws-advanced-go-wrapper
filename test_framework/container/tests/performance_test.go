//go:build performance
// +build performance

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
package test

import (
	awsDriver "awssql/driver"
	"awssql/property_util"
	"awssql/test_framework/container/test_utils"
	"awssql/utils"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var perfTestEnvironment *test_utils.TestEnvironment
var perfDataList []test_utils.PerfStat

const PERF_TEST_DEFAULT_REPEAT_TIMES = 5
const USE_SQL_DB_DEFAULT = true
const PERF_TEST_CONNECT_TIMEOUT = 3
const PERF_FAILOVER_TIMEOUT_MS = 120000

var failureDetectionTimeParams = [][]int64{
	{30000, 5000, 3, 5000},
	{30000, 5000, 3, 10000},
	{30000, 5000, 3, 15000},
	{30000, 5000, 3, 20000},
	{30000, 5000, 3, 25000},
	{30000, 5000, 3, 30000},
	{30000, 5000, 3, 35000},
	{30000, 5000, 3, 40000},
	{30000, 5000, 3, 45000},
	{30000, 5000, 3, 50000},

	{6000, 1000, 1, 1000},
	{6000, 1000, 1, 2000},
	{6000, 1000, 1, 3000},
	{6000, 1000, 1, 4000},
	{6000, 1000, 1, 5000},
	{6000, 1000, 1, 6000},
	{6000, 1000, 1, 7000},
	{6000, 1000, 1, 8000},
	{6000, 1000, 1, 9000},
	{6000, 1000, 1, 10000},
}

func TestPerformanceFailureDetectionTimeEfmEnabled(t *testing.T) {
	perfDataList = make([]test_utils.PerfStat, 0)
	var err error
	perfTestEnvironment, err = test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)

	useSqlDb := USE_SQL_DB_DEFAULT
	envValue := os.Getenv("USE_SQL_DB")

	if envValue != "" {
		useSqlDb = strings.EqualFold(envValue, "true") || strings.EqualFold(envValue, "1") || strings.EqualFold(envValue, "yes")
	}

	for i := 0; i < len(failureDetectionTimeParams); i++ {
		slog.Info(fmt.Sprintf("Testing EFM only Perf with the following parameters: '%v', '%v', '%v', '%v'",
			failureDetectionTimeParams[i][0],
			failureDetectionTimeParams[i][1],
			failureDetectionTimeParams[i][2],
			failureDetectionTimeParams[i][3]))
		executeFailureDetectionTimeEfmEnabled(
			failureDetectionTimeParams[i][0],
			failureDetectionTimeParams[i][1],
			failureDetectionTimeParams[i][2],
			failureDetectionTimeParams[i][3],
			useSqlDb,
		)
	}

	// Write report to xlsx file
	engine := perfTestEnvironment.Info().Request.Engine
	numInstances := len(perfTestEnvironment.Info().DatabaseInfo.Instances)
	fileName := fmt.Sprintf("EnhancedMonitoringOnly_Db_%v_Instances_%v_Plugins_efm.xlsx", engine, numInstances)
	sheetName := "EfmOnly"
	if !useSqlDb {
		fileName = "Wrapper_Conn_" + fileName
		sheetName = "Wrapper_Conn_" + sheetName
	}

	err = test_utils.WritePerfDataToFile(perfDataList,
		fileName,
		sheetName,
	)
	if err != nil {
		slog.Error("Could not write results to xlsx file")
		t.Fail()
	}
}

func TestPerformanceFailureDetectionTimeEfmAndFailoverEnabled(t *testing.T) {
	utils.SetPreparedHostFunc(func(host string) string {
		preparedHost := host
		const suffix = ".proxied"
		if strings.HasSuffix(host, suffix) {
			preparedHost = strings.TrimSuffix(host, suffix)
		}
		return preparedHost
	})
	slog.SetLogLoggerLevel(slog.LevelDebug)
	perfDataList = make([]test_utils.PerfStat, 0)
	var err error
	perfTestEnvironment, err = test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)

	useSqlDb := USE_SQL_DB_DEFAULT
	envValue := os.Getenv("USE_SQL_DB")

	if envValue != "" {
		useSqlDb = strings.EqualFold(envValue, "true") || strings.EqualFold(envValue, "1") || strings.EqualFold(envValue, "yes")
	}

	for i := 0; i < len(failureDetectionTimeParams); i++ {
		slog.Info(fmt.Sprintf("Testing EFM + Failover Perf with the following parameters: '%v', '%v', '%v', '%v'",
			failureDetectionTimeParams[i][0],
			failureDetectionTimeParams[i][1],
			failureDetectionTimeParams[i][2],
			failureDetectionTimeParams[i][3]))
		executeFailureDetectionTimeEfmAndFailoverEnabled(
			failureDetectionTimeParams[i][0],
			failureDetectionTimeParams[i][1],
			failureDetectionTimeParams[i][2],
			failureDetectionTimeParams[i][3],
			useSqlDb,
		)
	}

	// Write report to xlsx file
	engine := perfTestEnvironment.Info().Request.Engine
	numInstances := len(perfTestEnvironment.Info().DatabaseInfo.Instances)
	fileName := fmt.Sprintf("FailoverWithEnhancedMonitoring_Db_%v_Instances_%v_Plugins_efm_failover.xlsx", engine, numInstances)
	sheetName := "FailoverWithEfm"

	if !useSqlDb {
		fileName = "Wrapper_Conn_" + fileName
		sheetName = "Wrapper_Conn_" + sheetName
	}

	err = test_utils.WritePerfDataToFile(perfDataList,
		fileName,
		sheetName,
	)
	if err != nil {
		slog.Error("Could not write results to xlsx file")
		t.Fail()
	}
}

func connectWithRetry(dsn string) (driver.Conn, error) {
	maxRetries := 10
	var err error = nil
	for attempt := 1; attempt <= maxRetries; attempt++ {
		wrapperDriver := awsDriver.AwsWrapperDriver{}
		conn, err := wrapperDriver.Open(dsn)

		if err == nil {
			return conn, nil // Connection successful
		}
		slog.Debug(fmt.Sprintf("Could not connect on attempt # '%v'. Received an error '%v'", attempt, err.Error()))
		time.Sleep(time.Duration(1) * time.Second)
	}
	return nil, err
}

func connectWithRetryDb(dsn string) (*sql.DB, error) {
	maxRetries := 10
	var err error = nil
	for attempt := 1; attempt <= maxRetries; attempt++ {

		db, err := sql.Open("awssql", dsn)

		if err != nil {
			slog.Debug(fmt.Sprintf("Could not connect on attempt # '%v'. Received an error '%v'", attempt, err.Error()))
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}
		err = db.Ping()
		if err == nil {
			return db, nil
		}
		slog.Debug(fmt.Sprintf("Could not connect on attempt # '%v'. Received an error '%v'", attempt, err.Error()))
		time.Sleep(time.Duration(1) * time.Second)
	}
	return nil, err
}

func executeFailureDetectionTimeEfmEnabled(
	detectionTimeMs int64,
	detectionIntervalMs int64,
	detectionCount int64,
	sleepDelayMs int64,
	useSqlDb bool,
) {
	testEnvironment, _ := test_utils.GetCurrentTestEnvironment()

	props := initPerfProps(testEnvironment)

	property_util.PLUGINS.Set(props, "efm")
	property_util.FAILURE_DETECTION_TIME_MS.Set(props, strconv.FormatInt(detectionTimeMs, 10))
	property_util.FAILURE_DETECTION_INTERVAL_MS.Set(props, strconv.FormatInt(detectionIntervalMs, 10))
	property_util.FAILURE_DETECTION_COUNT.Set(props, strconv.FormatInt(detectionCount, 10))

	executePerformanceTest(props, detectionTimeMs, detectionIntervalMs, detectionCount, sleepDelayMs, useSqlDb)
}

func executeFailureDetectionTimeEfmAndFailoverEnabled(
	detectionTimeMs int64,
	detectionIntervalMs int64,
	detectionCount int64,
	sleepDelayMs int64,
	useSqlDb bool,
) {
	testEnvironment, _ := test_utils.GetCurrentTestEnvironment()

	props := initPerfProps(testEnvironment)

	property_util.PLUGINS.Set(props, "efm,failover")
	property_util.FAILURE_DETECTION_TIME_MS.Set(props, strconv.FormatInt(detectionTimeMs, 10))
	property_util.FAILURE_DETECTION_INTERVAL_MS.Set(props, strconv.FormatInt(detectionIntervalMs, 10))
	property_util.FAILURE_DETECTION_COUNT.Set(props, strconv.FormatInt(detectionCount, 10))
	property_util.FAILOVER_TIMEOUT_MS.Set(props, strconv.FormatInt(PERF_FAILOVER_TIMEOUT_MS, 10))
	property_util.FAILOVER_MODE.Set(props, "strict-reader")

	executePerformanceTest(props, detectionTimeMs, detectionIntervalMs, detectionCount, sleepDelayMs, useSqlDb)
}

func initPerfProps(testEnvironment *test_utils.TestEnvironment) map[string]string {
	monitoringConnectTimeoutSeconds := strconv.Itoa(PERF_TEST_CONNECT_TIMEOUT)
	monitoringConnectTimeoutParameterName := property_util.MONITORING_PROPERTY_PREFIX
	var driverProtocol = ""
	switch testEnvironment.Info().Request.Engine {
	case test_utils.PG:
		monitoringConnectTimeoutParameterName = monitoringConnectTimeoutParameterName + "connect_timeout"
		driverProtocol = utils.PGX_DRIVER_PROTOCOL
	case test_utils.MYSQL:
		monitoringConnectTimeoutParameterName = monitoringConnectTimeoutParameterName + "readTimeout"
		monitoringConnectTimeoutSeconds = monitoringConnectTimeoutSeconds + "s"
		driverProtocol = utils.MYSQL_DRIVER_PROTOCOL
	}

	props := map[string]string{
		property_util.HOST.Name:                          testEnvironment.Info().ProxyDatabaseInfo.WriterInstanceEndpoint(),
		property_util.PORT.Name:                          strconv.Itoa(testEnvironment.Info().ProxyDatabaseInfo.InstanceEndpointPort),
		property_util.USER.Name:                          testEnvironment.Info().DatabaseInfo.Username,
		property_util.PASSWORD.Name:                      testEnvironment.Info().DatabaseInfo.Password,
		property_util.CLUSTER_INSTANCE_HOST_PATTERN.Name: "?." + testEnvironment.Info().ProxyDatabaseInfo.InstanceEndpointSuffix + ":" + strconv.Itoa(testEnvironment.Info().ProxyDatabaseInfo.InstanceEndpointPort),
		property_util.CLUSTER_ID.Name:                    "test-cluster-id",
		property_util.DRIVER_PROTOCOL.Name:               driverProtocol,
		monitoringConnectTimeoutParameterName:            monitoringConnectTimeoutSeconds,
	}

	return props
}

func executePerformanceTest(
	props map[string]string,
	detectionTimeMs int64,
	detectionIntervalMs int64,
	detectionCount int64,
	sleepDelayMs int64,
	useSqlDb bool,
) {
	data := PerfStatMonitoring{
		paramDetectionTime:            detectionTimeMs,
		paramDetectionInterval:        detectionIntervalMs,
		paramDetectionCount:           detectionCount,
		paramNetworkOutageDelayMillis: sleepDelayMs,
	}

	repeatTimes := PERF_TEST_DEFAULT_REPEAT_TIMES
	envValue := os.Getenv("REPEAT_TIMES")
	if envValue != "" {
		val, err := strconv.Atoi(envValue)
		if err == nil {
			repeatTimes = val
		}
	}

	if useSqlDb {
		slog.Debug("Testing with SQL Db")
		doMeasurePerformanceDb(sleepDelayMs, repeatTimes, &data, props)
	} else {
		slog.Debug("Testing without SQL Db")
		doMeasurePerformance(sleepDelayMs, repeatTimes, &data, props)
	}

	slog.Debug(fmt.Sprintf("Run completed. Got the following data: '%v'", data))
	perfDataList = append(perfDataList, data)
}

func calculateStats(nums []int64) (min, max, avg int64) {
	min, max = int64(nums[0]), int64(nums[0])
	var sum int64

	for _, n := range nums {
		val := int64(n)
		if val < min {
			min = val
		}
		if val > max {
			max = val
		}
		sum += val
	}

	avg = sum / int64(len(nums)) // Truncated average
	return
}

func doMeasurePerformance(
	sleepDelayMs int64,
	repeatTimes int,
	data *PerfStatMonitoring,
	props map[string]string,
) {
	var elapsedTimeMs []int64
	dsn := test_utils.GetDsn(perfTestEnvironment, props)
	engine := perfTestEnvironment.Info().Request.Engine
	deployment := perfTestEnvironment.Info().Request.Deployment
	sleepDurationBetweenRepeats := time.Duration(25) * time.Second

	for i := 0; i < repeatTimes; i++ {
		// Let monitoring threads recover from previous network outage
		time.Sleep(sleepDurationBetweenRepeats)

		conn, _ := connectWithRetry(dsn)
		defer conn.Close()

		instanceId, _ := test_utils.ExecuteInstanceQuery(engine, deployment, conn)

		downTimeStartCh := make(chan time.Time)
		// Disable connectivity in another thread after sleep delay
		go func() {
			time.Sleep(time.Duration(sleepDelayMs) * time.Millisecond)
			proxyInfo := perfTestEnvironment.ProxyInfos()[instanceId]
			test_utils.DisableProxyConnectivity(proxyInfo)
			slog.Info(fmt.Sprintf("Disabled connectivity and sent down time to channel at: '%v'", time.Now()))
			downTimeStartCh <- time.Now()
			close(downTimeStartCh)
		}()

		_, err := test_utils.ExecuteQuery(engine, conn, test_utils.GetSleepSql(engine, 70), 1200)

		if err == nil {
			slog.Error("Did not receive error while executing query. Skipping results")
			test_utils.EnableAllConnectivity(true)
			continue
		}

		slog.Info(fmt.Sprintf("Waiting to receive from channel at: '%v'", time.Now()))
		downTimeStart := <-downTimeStartCh
		slog.Info(fmt.Sprintf("Received from channel at: '%v'", time.Now()))

		failureTimeMs := time.Since(downTimeStart).Milliseconds()
		elapsedTimeMs = append(elapsedTimeMs, failureTimeMs)
		test_utils.EnableAllConnectivity(true)
	}
	slog.Debug(fmt.Sprintf("Elapsed time for run: '%v'", elapsedTimeMs))
	min, max, avg := calculateStats(elapsedTimeMs)
	data.minFailureDetectionTimeMillis = min
	data.maxFailureDetectionTimeMillis = max
	data.avgFailureDetectionTimeMillis = avg
}

func doMeasurePerformanceDb(
	sleepDelayMs int64,
	repeatTimes int,
	data *PerfStatMonitoring,
	props map[string]string,
) {
	var elapsedTimeMs []int64
	dsn := test_utils.GetDsn(perfTestEnvironment, props)
	engine := perfTestEnvironment.Info().Request.Engine
	deployment := perfTestEnvironment.Info().Request.Deployment
	sleepDurationBetweenRepeats := time.Duration(25) * time.Second

	for i := 0; i < repeatTimes; i++ {
		// Let monitoring threads recover from previous network outage
		time.Sleep(sleepDurationBetweenRepeats)

		conn, _ := connectWithRetryDb(dsn)
		defer conn.Close()

		instanceId, _ := test_utils.ExecuteInstanceQueryDB(engine, deployment, conn)

		downTimeStartCh := make(chan time.Time)
		// Disable connectivity in another thread after sleep delay
		go func() {
			time.Sleep(time.Duration(sleepDelayMs) * time.Millisecond)
			proxyInfo := perfTestEnvironment.ProxyInfos()[instanceId]
			test_utils.DisableProxyConnectivity(proxyInfo)
			slog.Info(fmt.Sprintf("Disabled connectivity and sent down time to channel at: '%v'", time.Now()))
			downTimeStartCh <- time.Now()
			close(downTimeStartCh)
		}()

		_, err := test_utils.ExecuteQueryDB(engine, conn, test_utils.GetSleepSql(engine, 70), 1200)

		if err == nil {
			slog.Error("Did not receive error while executing query. Skipping results")
			test_utils.EnableAllConnectivity(true)
			continue
		}

		slog.Info(fmt.Sprintf("Waiting to receive from channel at: '%v'", time.Now()))
		downTimeStart := <-downTimeStartCh
		slog.Info(fmt.Sprintf("Received from channel at: '%v'", time.Now()))

		failureTimeMs := time.Since(downTimeStart).Milliseconds()
		elapsedTimeMs = append(elapsedTimeMs, failureTimeMs)
		test_utils.EnableAllConnectivity(true)
	}
	slog.Debug(fmt.Sprintf("Elapsed time for run: '%v'", elapsedTimeMs))
	min, max, avg := calculateStats(elapsedTimeMs)
	data.minFailureDetectionTimeMillis = min
	data.maxFailureDetectionTimeMillis = max
	data.avgFailureDetectionTimeMillis = avg
}

// Failure detection with failover and efm enabled.
type PerfStatMonitoring struct {
	paramNetworkOutageDelayMillis int64
	minFailureDetectionTimeMillis int64
	maxFailureDetectionTimeMillis int64
	avgFailureDetectionTimeMillis int64
	paramDetectionTime            int64
	paramDetectionInterval        int64
	paramDetectionCount           int64
}

func (p PerfStatMonitoring) WriteHeader() []string {
	return []string{
		"FailureDetectionGraceTime",
		"FailureDetectionInterval",
		"FailureDetectionCount",
		"NetworkOutageDelayMillis",
		"MinFailureDetectionTimeMillis",
		"MaxFailureDetectionTimeMillis",
		"AvgFailureDetectionTimeMillis",
	}
}

func (p PerfStatMonitoring) WriteData() []int64 {
	return []int64{
		p.paramDetectionTime,
		p.paramDetectionInterval,
		p.paramDetectionCount,
		p.paramNetworkOutageDelayMillis,
		p.minFailureDetectionTimeMillis,
		p.maxFailureDetectionTimeMillis,
		p.avgFailureDetectionTimeMillis,
	}
}
