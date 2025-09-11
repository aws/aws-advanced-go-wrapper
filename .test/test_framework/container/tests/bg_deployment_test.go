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
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins/bg"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/olekukonko/tablewriter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	includeClusterEndpoints    = false
	includeWriterAndReaderOnly = false
	testClusterId              = "test-cluster-id"
	mysqlBgStatusQuery         = "SELECT role, status FROM mysql.rds_topology"
	pgAuroraBgStatusQuery      = "SELECT role, status FROM get_blue_green_fast_switchover_metadata('aws_advanced_go_wrapper')"
	pgRdsBgStatusQuery         = "SELECT role, status FROM rds_tools.show_topology('aws_advanced_go_wrapper-1.0.0')"
)

// Represents a time measurement with optional error and hold time.
type TimeHolder struct {
	StartTime int64
	EndTime   int64
	HoldNano  time.Duration
	Error     string
}

// Holds all the timing and status results for a single host.
type BlueGreenResults struct {
	StartTime                               int64
	ThreadsSyncTime                         int64
	BgTriggerTime                           int64
	DirectBlueLostConnectionTime            int64
	DirectBlueIdleLostConnectionTime        int64
	WrapperBlueIdleLostConnectionTime       int64
	WrapperGreenLostConnectionTime          int64
	DnsBlueChangedTime                      int64
	DnsBlueError                            string
	DnsGreenRemovedTime                     int64
	GreenNodeChangeNameTime                 int64
	BlueStatusTime                          sync.Map
	GreenStatusTime                         sync.Map
	BlueWrapperConnectTimes                 []TimeHolder
	BlueWrapperExecuteTimes                 []TimeHolder
	GreenWrapperExecuteTimes                []TimeHolder
	GreenDirectIamWithBlueNodeConnectTimes  []TimeHolder
	GreenDirectIamWithGreenNodeConnectTimes []TimeHolder

	// Mutexes for slice operations
	BlueWrapperConnectTimesMutex                 sync.Mutex
	BlueWrapperExecuteTimesMutex                 sync.Mutex
	GreenWrapperExecuteTimesMutex                sync.Mutex
	GreenDirectIamWithBlueNodeConnectTimesMutex  sync.Mutex
	GreenDirectIamWithGreenNodeConnectTimesMutex sync.Mutex
}

// Manages the overall test execution.
type BlueGreenTestSuite struct {
	results             sync.Map
	unhandledExceptions []error
	exceptionsMutex     sync.Mutex
	auroraUtil          *test_utils.AuroraTestUtility
}

func NewBlueGreenTestSuite() *BlueGreenTestSuite {
	return &BlueGreenTestSuite{
		results:             sync.Map{},
		unhandledExceptions: make([]error, 0),
	}
}

func (suite *BlueGreenTestSuite) addUnhandledException(err error) {
	suite.exceptionsMutex.Lock()
	defer suite.exceptionsMutex.Unlock()
	suite.unhandledExceptions = append(suite.unhandledExceptions, err)
}

func (suite *BlueGreenTestSuite) getResults(hostId string) *BlueGreenResults {
	if result, ok := suite.results.Load(hostId); ok {
		return result.(*BlueGreenResults)
	}
	return nil
}

func (suite *BlueGreenTestSuite) setResults(hostId string, results *BlueGreenResults) {
	suite.results.Store(hostId, results)
}

/*
NOTE: this test requires manual verification to fully verify proper B/G behavior.

	PASS criteria:
	- automatic check: test passes
	- manual check: test logs contain the switchover final summary table with the following events and similar time
	offset values:
	----------------------------------------------------------------------------
	timestamp                         time offset (ms)                     event
	----------------------------------------------------------------------------
	2025-09-05T19:49:39.715Z         -219228 ms                         CREATED
	2025-09-05T19:53:16.405Z           -2539 ms                     PREPARATION
	2025-09-05T19:53:18.944Z               0 ms                     IN_PROGRESS
	2025-09-05T19:53:22.802Z            3858 ms                            POST
	2025-09-05T19:53:32.442Z           13497 ms          Green topology changed
	2025-09-05T19:53:47.302Z           28357 ms                Blue DNS updated
	2025-09-05T19:54:00.871Z           41926 ms               Green DNS removed
	2025-09-05T19:54:00.871Z           41926 ms                       COMPLETED
	----------------------------------------------------------------------------
*/
func TestBlueGreenSwitchover(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()
	environment, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)

	// Skip test if Blue/Green deployment feature is not enabled
	test_utils.RequireTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.BLUE_GREEN_DEPLOYMENT)
	suite := NewBlueGreenTestSuite()
	suite.auroraUtil = test_utils.NewAuroraTestUtility(environment.Info())
	err = suite.runSwitchoverTest(t, environment)
	require.NoError(t, err)
}

func (suite *BlueGreenTestSuite) runSwitchoverTest(t *testing.T, environment *test_utils.TestEnvironment) error {
	// Clear previous results
	suite.results = sync.Map{}
	suite.unhandledExceptions = make([]error, 0)

	startTimeNano := time.Now().UnixNano()

	// Create stop channel and wait groups
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	info := environment.Info()
	dbName := info.DatabaseInfo.DefaultDbName
	testInstance := info.DatabaseInfo.Instances[0]

	// Get Blue/Green endpoints
	topologyInstances, err := suite.getBlueGreenEndpoints(info.DatabaseInfo.BlueGreenDeploymentId, environment)
	if err != nil {
		return fmt.Errorf("failed to get Blue/Green endpoints: %w", err)
	}

	slog.Info("Topology instances", "instances", strings.Join(topologyInstances, "\n"))

	// Initialize results for each host
	for _, host := range topologyInstances {
		hostId := strings.Split(host, ".")[0]
		assert.NotEmpty(t, hostId)

		results := &BlueGreenResults{
			StartTime: startTimeNano,
		}
		suite.setResults(hostId, results)

		// Start monitoring goroutines for blue (non-green) hosts
		if utils.IsNotGreenAndNotOldInstance(host) {
			wg.Add(1)
			go suite.directTopologyMonitor(hostId, host, testInstance.Port(), dbName, stopChan, &wg, environment)

			wg.Add(1)
			go suite.directBlueConnectivityMonitor(hostId, host, testInstance.Port(), dbName, stopChan, &wg, environment)

			wg.Add(1)
			go suite.directBlueIdleConnectivityMonitor(hostId, host, testInstance.Port(), dbName, stopChan, &wg, environment)

			wg.Add(1)
			go suite.wrapperBlueIdleConnectivityMonitor(hostId, host, testInstance.Port(), dbName, stopChan, &wg, environment)

			wg.Add(1)
			go suite.wrapperBlueExecutingConnectivityMonitor(hostId, host, testInstance.Port(), dbName, stopChan, &wg, environment)

			wg.Add(1)
			go suite.wrapperBlueNewConnectionMonitor(hostId, host, testInstance.Port(), dbName, stopChan, &wg, environment)

			wg.Add(1)
			go suite.blueDnsMonitor(hostId, host, stopChan, &wg)
		}

		// Start monitoring goroutines for green hosts
		if utils.IsGreenInstance(host) {
			wg.Add(1)
			go suite.directTopologyMonitor(hostId, host, testInstance.Port(), dbName, stopChan, &wg, environment)

			wg.Add(1)
			go suite.wrapperGreenConnectivityMonitor(hostId, host, testInstance.Port(), dbName, stopChan, &wg, environment)

			wg.Add(1)
			go suite.greenDnsMonitor(hostId, host, stopChan, &wg)

			if slices.Contains(environment.Info().Request.Features, test_utils.IAM) {
				wg.Add(1)
				go suite.greenIamConnectivityMonitor(
					hostId, "BlueHostToken",
					utils.RemoveGreenInstancePrefix(host),
					testInstance.Port(), dbName, stopChan, &wg, environment,
					&results.GreenDirectIamWithBlueNodeConnectTimes,
					&results.GreenDirectIamWithBlueNodeConnectTimesMutex,
					false, true)

				wg.Add(1)
				go suite.greenIamConnectivityMonitor(
					hostId, "GreenHostToken", host,
					testInstance.Port(), dbName, stopChan, &wg, environment,
					&results.GreenDirectIamWithGreenNodeConnectTimes,
					&results.GreenDirectIamWithGreenNodeConnectTimesMutex,
					true, false)
			}
		}
	}

	// Start Blue/Green switchover trigger
	wg.Add(1)
	go suite.blueGreenSwitchoverTrigger(info.DatabaseInfo.BlueGreenDeploymentId, &wg)

	// Wait for test completion
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("All goroutines completed")
	case <-time.After(10 * time.Minute):
		slog.Info("Test timeout reached, stopping goroutines")
		close(stopChan)
		// Wait additional time for graceful shutdown
		time.Sleep(time.Minute)
	}

	// Stop all goroutines
	select {
	case <-stopChan:
		// Already closed
	default:
		close(stopChan)
	}

	// Wait for goroutines to finish
	time.Sleep(5 * time.Second)

	// Verify that all hosts have bg_trigger_time set
	suite.results.Range(func(key, value interface{}) bool {
		hostId := key.(string)
		results := value.(*BlueGreenResults)
		assert.Greater(t, atomic.LoadInt64(&results.BgTriggerTime), int64(0),
			"bg_trigger_time for %s should be greater than 0", hostId)
		return true
	})

	suite.printMetrics()

	// Check for unhandled exceptions
	if len(suite.unhandledExceptions) > 0 {
		suite.logUnhandledExceptions()
		t.Fatal("There are unhandled exceptions")
	}

	suite.assertTest(t)

	slog.Info("Blue/Green switchover test completed successfully")
	return nil
}

func (suite *BlueGreenTestSuite) getBlueGreenEndpoints(blueGreenId string, environment *test_utils.TestEnvironment) ([]string, error) {
	ctx := context.Background()

	bgDeployment, err := suite.auroraUtil.GetBlueGreenDeployment(ctx, blueGreenId)
	if err != nil {
		return nil, fmt.Errorf("Blue/Green deployment with ID '%s' not found: %w", blueGreenId, err)
	}

	deployment := environment.Info().Request.Deployment

	switch deployment {
	case test_utils.RDS_MULTI_AZ_INSTANCE:
		blueInstance, err := suite.auroraUtil.GetRdsInstanceInfoByArn(ctx, *bgDeployment.Source)
		if err != nil {
			return nil, fmt.Errorf("blue instance not found: %w", err)
		}

		greenInstance, err := suite.auroraUtil.GetRdsInstanceInfoByArn(ctx, *bgDeployment.Target)
		if err != nil {
			return nil, fmt.Errorf("green instance not found: %w", err)
		}

		return []string{*blueInstance.Endpoint.Address, *greenInstance.Endpoint.Address}, nil

	case test_utils.AURORA:
		endpoints := make([]string, 0)

		_, err := suite.auroraUtil.GetClusterByArn(ctx, *bgDeployment.Source)
		if err != nil {
			return nil, fmt.Errorf("blue cluster not found: %w", err)
		}

		info := environment.Info()
		instances := info.DatabaseInfo.Instances

		if includeClusterEndpoints {
			endpoints = append(endpoints, info.DatabaseInfo.ClusterEndpoint)
		}

		if includeWriterAndReaderOnly {
			endpoints = append(endpoints, instances[0].Host())
			if len(instances) > 1 {
				endpoints = append(endpoints, instances[1].Host())
			}
		} else {
			for _, instance := range instances {
				endpoints = append(endpoints, instance.Host())
			}
		}

		greenCluster, err := suite.auroraUtil.GetClusterByArn(ctx, *bgDeployment.Target)
		if err != nil {
			return nil, fmt.Errorf("green cluster not found: %w", err)
		}

		if includeClusterEndpoints {
			endpoints = append(endpoints, *greenCluster.Endpoint)
		}

		instanceIds, err := suite.auroraUtil.GetRdsInstanceIds(
			environment.Info().Request.Engine,
			deployment,
			*greenCluster.Endpoint,
			info.DatabaseInfo.ClusterEndpointPort,
			info.DatabaseInfo.DefaultDbName,
			info.DatabaseInfo.Username,
			info.DatabaseInfo.Password)
		if err != nil || len(instanceIds) == 0 {
			return nil, fmt.Errorf("cannot find green cluster instances: %w", err)
		}

		instancePattern := utils.GetRdsInstanceHostPattern(*greenCluster.Endpoint)
		if includeWriterAndReaderOnly {
			endpoints = append(endpoints, strings.Replace(instancePattern, "?", instanceIds[0], 1))
			if len(instanceIds) > 1 {
				endpoints = append(endpoints, strings.Replace(instancePattern, "?", instanceIds[1], 1))
			}
		} else {
			for _, instanceId := range instanceIds {
				endpoints = append(endpoints, strings.Replace(instancePattern, "?", instanceId, 1))
			}
		}

		return endpoints, nil

	default:
		return nil, fmt.Errorf("unsupported Blue/Green engine deployment: %v", deployment)
	}
}

func (suite *BlueGreenTestSuite) openConnectionWithRetry(dsn string, environment *test_utils.TestEnvironment) (driver.Conn, error) {
	var conn driver.Conn
	var err error

	wrapperDriver := test_utils.NewWrapperDriver(environment.Info().Request.Engine)

	for i := 0; i < 10; i++ {
		conn, err = wrapperDriver.Open(dsn)
		if err == nil {
			if pingErr := pingConn(conn); pingErr == nil {
				return conn, nil
			}

			_ = conn.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("failed to connect after 10 attempts: %w", err)
}

func (suite *BlueGreenTestSuite) closeConnection(conn driver.Conn) {
	if conn != nil {
		if err := conn.Close(); err != nil {
			slog.Warn(fmt.Sprintf("Error closing connection: %v", err))
		}
	}
}

func (suite *BlueGreenTestSuite) getWrapperConnectionProperties(environment *test_utils.TestEnvironment) map[string]string {
	props := make(map[string]string)
	props["clusterId"] = testClusterId

	engine := environment.Info().Request.Engine
	deployment := environment.Info().Request.Deployment

	switch deployment {
	case test_utils.AURORA:
		switch engine {
		case test_utils.MYSQL:
			props[property_util.DIALECT.Name] = "aurora-mysql"
		case test_utils.PG:
			props[property_util.DIALECT.Name] = "aurora-pg"
		}
	case test_utils.RDS_MULTI_AZ_INSTANCE:
		switch engine {
		case test_utils.MYSQL:
			props[property_util.DIALECT.Name] = "rds-mysql"
		case test_utils.PG:
			props[property_util.DIALECT.Name] = "rds-pg"
		}
	}

	if slices.Contains(environment.Info().Request.Features, test_utils.IAM) {
		props[property_util.PLUGINS.Name] = "bg,iam"
		props[property_util.IAM_REGION.Name] = environment.Info().Region
		props[property_util.USER.Name] = environment.Info().IamUsername
		props = addMySQLIamHandlingIfNecessary(environment, props)
	} else {
		props[property_util.PLUGINS.Name] = "bg"
	}

	return props
}

func (suite *BlueGreenTestSuite) getTopologyQuery(environment *test_utils.TestEnvironment) string {
	engine := environment.Info().Request.Engine
	deployment := environment.Info().Request.Deployment

	switch engine {
	case test_utils.MYSQL:
		return mysqlBgStatusQuery
	case test_utils.PG:
		switch deployment {
		case test_utils.AURORA:
			return pgAuroraBgStatusQuery
		case test_utils.RDS_MULTI_AZ_INSTANCE:
			return pgRdsBgStatusQuery
		default:
			panic(fmt.Sprintf("unsupported Blue/Green database engine deployment: %v", deployment))
		}
	default:
		panic(fmt.Sprintf("unsupported database engine: %v", engine))
	}
}

func (suite *BlueGreenTestSuite) directTopologyMonitor(hostId, host string, port int, dbName string,
	stopChan <-chan struct{}, wg *sync.WaitGroup, environment *test_utils.TestEnvironment) {
	defer wg.Done()

	var conn driver.Conn
	query := suite.getTopologyQuery(environment)

	defer func() {
		suite.closeConnection(conn)
		slog.Debug("DirectTopology monitor completed", "hostId", hostId)
	}()

	dsn := test_utils.GetDsn(environment, map[string]string{
		property_util.HOST.Name:     host,
		property_util.PORT.Name:     strconv.Itoa(port),
		property_util.DATABASE.Name: dbName,
		property_util.PLUGINS.Name:  "none",
	})

	var err error
	conn, err = suite.openConnectionWithRetry(dsn, environment)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("DirectTopology @ %s: failed to connect: %w", hostId, err))
		return
	}

	slog.Debug("DirectTopology connection opened", "hostId", hostId)

	time.Sleep(1 * time.Second)

	slog.Debug("DirectTopology starting BG status monitoring", "hostId", hostId)

	endTime := time.Now().Add(10 * time.Minute)
	results := suite.getResults(hostId)

	for {
		select {
		case <-stopChan:
			return
		default:
			if time.Now().After(endTime) {
				return
			}
		}

		if conn == nil {
			conn, err = suite.openConnectionWithRetry(dsn, environment)
			if err != nil {
				slog.Debug("DirectTopology failed to reconnect", "hostId", hostId, "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
			slog.Debug("DirectTopology connection re-opened", "hostId", hostId)
		}

		rows, err := queryConn(conn, query)
		if err != nil {
			slog.Debug("DirectTopology query failed", "hostId", hostId, "error", err)
			suite.closeConnection(conn)
			conn = nil
			time.Sleep(100 * time.Millisecond)
			continue
		}

		row := make([]driver.Value, len(rows.Columns()))
		for rows.Next(row) == nil {
			if len(row) < 2 {
				slog.Debug("DirectTopology query scan failed, too few values in row", "hostId", hostId)
				continue
			}
			var role, status string
			if environment.Info().Request.Engine == test_utils.MYSQL {
				roleAsInt, ok := row[0].([]uint8)
				statusAsInt, ok2 := row[1].([]uint8)
				if !ok || !ok2 {
					slog.Debug("DirectTopology query failed to cast", "hostId", hostId)
					continue
				}
				role = string(roleAsInt)
				status = string(statusAsInt)
			} else {
				var ok, ok2 bool
				role, ok = row[0].(string)
				status, ok2 = row[1].(string)
				if !ok || !ok2 {
					slog.Debug("DirectTopology query failed to cast", "hostId", hostId)
					continue
				}
			}

			isGreen := driver_infrastructure.ParseRole(role) == driver_infrastructure.TARGET
			currentTime := time.Now().UnixNano()

			if isGreen {
				if _, loaded := results.GreenStatusTime.LoadOrStore(status, currentTime); !loaded {
					slog.Debug("DirectTopology status changed", "hostId", hostId, "status", status, "type", "green")
				}
			} else {
				if _, loaded := results.BlueStatusTime.LoadOrStore(status, currentTime); !loaded {
					slog.Debug("DirectTopology status changed", "hostId", hostId, "status", status, "type", "blue")
				}
			}
		}
		_ = rows.Close()

		time.Sleep(100 * time.Millisecond)
	}
}

func (suite *BlueGreenTestSuite) directBlueConnectivityMonitor(hostId, host string, port int, dbName string,
	stopChan <-chan struct{}, wg *sync.WaitGroup, environment *test_utils.TestEnvironment) {
	defer wg.Done()

	var conn driver.Conn

	defer func() {
		suite.closeConnection(conn)
		slog.Debug("DirectBlueConnectivity monitor completed", "hostId", hostId)
	}()

	dsn := test_utils.GetDsn(environment, map[string]string{
		property_util.HOST.Name:     host,
		property_util.PORT.Name:     strconv.Itoa(port),
		property_util.DATABASE.Name: dbName,
		property_util.PLUGINS.Name:  "none",
	})

	var err error
	conn, err = suite.openConnectionWithRetry(dsn, environment)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("DirectBlueConnectivity @ %s: failed to connect: %w", hostId, err))
		return
	}

	slog.Debug("DirectBlueConnectivity connection opened", "hostId", hostId)

	time.Sleep(1 * time.Second)

	slog.Debug("DirectBlueConnectivity starting connectivity monitoring", "hostId", hostId)

	results := suite.getResults(hostId)

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		err := execConn(conn, "SELECT 1")
		if err != nil {
			slog.Debug("DirectBlueConnectivity query failed", "hostId", hostId, "error", err)
			atomic.StoreInt64(&results.DirectBlueLostConnectionTime, time.Now().UnixNano())
			return
		}

		time.Sleep(1 * time.Second)
	}
}

func (suite *BlueGreenTestSuite) directBlueIdleConnectivityMonitor(hostId, host string, port int, dbName string,
	stopChan <-chan struct{}, wg *sync.WaitGroup, environment *test_utils.TestEnvironment) {
	defer wg.Done()

	var conn driver.Conn

	defer func() {
		suite.closeConnection(conn)
		slog.Debug("DirectBlueIdleConnectivity monitor completed", "hostId", hostId)
	}()

	dsn := test_utils.GetDsn(environment, map[string]string{
		property_util.HOST.Name:     host,
		property_util.PORT.Name:     strconv.Itoa(port),
		property_util.DATABASE.Name: dbName,
		property_util.PLUGINS.Name:  "none",
	})

	var err error
	conn, err = suite.openConnectionWithRetry(dsn, environment)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("DirectBlueIdleConnectivity @ %s: failed to connect: %w", hostId, err))
		return
	}

	slog.Debug("DirectBlueIdleConnectivity connection opened", "hostId", hostId)

	time.Sleep(1 * time.Second)

	slog.Debug("DirectBlueIdleConnectivity starting connectivity monitoring", "hostId", hostId)

	results := suite.getResults(hostId)

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		// Check if connection is still alive
		err := pingConn(conn)
		if err != nil {
			slog.Debug("DirectBlueIdleConnectivity ping failed", "hostId", hostId, "error", err)
			atomic.StoreInt64(&results.DirectBlueIdleLostConnectionTime, time.Now().UnixNano())
			return
		}

		time.Sleep(1 * time.Second)
	}
}

func (suite *BlueGreenTestSuite) wrapperBlueExecutingConnectivityMonitor(hostId, host string, port int, dbName string,
	stopChan <-chan struct{}, wg *sync.WaitGroup, environment *test_utils.TestEnvironment) {
	defer wg.Done()

	var conn driver.Conn

	defer func() {
		suite.closeConnection(conn)
		slog.Debug("WrapperBlueExecute monitor completed", "hostId", hostId)
	}()

	props := suite.getWrapperConnectionProperties(environment)
	props[property_util.HOST.Name] = host
	props[property_util.PORT.Name] = strconv.Itoa(port)
	props[property_util.DATABASE.Name] = dbName

	dsn := test_utils.GetDsn(environment, props)

	var err error
	conn, err = suite.openConnectionWithRetry(dsn, environment)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("WrapperBlueExecute @ %s: failed to connect: %w", hostId, err))
		return
	}

	slog.Debug("WrapperBlueExecute connection opened", "hostId", hostId)

	time.Sleep(1 * time.Second)

	slog.Debug("WrapperBlueExecute starting connectivity monitoring", "hostId", hostId)

	results := suite.getResults(hostId)

	var sleepQuery string
	switch environment.Info().Request.Engine {
	case test_utils.MYSQL:
		sleepQuery = "SELECT SLEEP(5)"
	case test_utils.PG:
		sleepQuery = "SELECT pg_sleep(5)"
	default:
		suite.addUnhandledException(fmt.Errorf("unsupported database engine: %v", environment.Info().Request.Engine))
		return
	}

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		startTime := time.Now().UnixNano()

		var holdTime time.Duration
		if awsConn, ok := conn.(*awsDriver.AwsWrapperConn); ok {
			if bgPlugin := awsConn.UnwrapPlugin(driver_infrastructure.BLUE_GREEN_PLUGIN_CODE); bgPlugin != nil {
				if blueGreenPlugin, ok := bgPlugin.(*bg.BlueGreenPlugin); ok {
					holdTime = blueGreenPlugin.GetHoldTimeNano()
				}
			}
		}

		err := execConn(conn, sleepQuery)
		endTime := time.Now().UnixNano()

		timeHolder := TimeHolder{
			StartTime: startTime,
			EndTime:   endTime,
			HoldNano:  holdTime,
		}

		if err != nil {
			timeHolder.Error = err.Error()
			// Check if connection is closed
			if pingConn(conn) != nil {
				results.BlueWrapperExecuteTimesMutex.Lock()
				results.BlueWrapperExecuteTimes = append(results.BlueWrapperExecuteTimes, timeHolder)
				results.BlueWrapperExecuteTimesMutex.Unlock()
				return
			}
		}

		results.BlueWrapperExecuteTimesMutex.Lock()
		results.BlueWrapperExecuteTimes = append(results.BlueWrapperExecuteTimes, timeHolder)
		results.BlueWrapperExecuteTimesMutex.Unlock()

		time.Sleep(1 * time.Second)
	}
}

func (suite *BlueGreenTestSuite) wrapperBlueNewConnectionMonitor(hostId, host string, port int, dbName string,
	stopChan <-chan struct{}, wg *sync.WaitGroup, environment *test_utils.TestEnvironment) {
	defer wg.Done()

	defer func() {
		slog.Debug("WrapperBlueNewConnection monitor completed", "hostId", hostId)
	}()

	props := suite.getWrapperConnectionProperties(environment)
	props[property_util.HOST.Name] = host
	props[property_util.PORT.Name] = strconv.Itoa(port)
	props[property_util.DATABASE.Name] = dbName

	dsn := test_utils.GetDsn(environment, props)

	time.Sleep(1 * time.Second)

	slog.Debug("WrapperBlueNewConnection starting connectivity monitoring", "hostId", hostId)

	results := suite.getResults(hostId)

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		startTime := time.Now().UnixNano()

		conn, err := suite.openConnectionWithRetry(dsn, environment)
		endTime := time.Now().UnixNano()

		timeHolder := TimeHolder{
			StartTime: startTime,
			EndTime:   endTime,
		}

		if conn != nil {
			if awsConn, ok := conn.(*awsDriver.AwsWrapperConn); ok {
				if bgPlugin := awsConn.UnwrapPlugin(driver_infrastructure.BLUE_GREEN_PLUGIN_CODE); bgPlugin != nil {
					if blueGreenPlugin, ok := bgPlugin.(*bg.BlueGreenPlugin); ok {
						timeHolder.HoldNano = blueGreenPlugin.GetHoldTimeNano()
					}
				}
			}
		}

		if err != nil {
			timeHolder.Error = err.Error()
		}

		results.BlueWrapperConnectTimesMutex.Lock()
		results.BlueWrapperConnectTimes = append(results.BlueWrapperConnectTimes, timeHolder)
		results.BlueWrapperConnectTimesMutex.Unlock()

		suite.closeConnection(conn)
		time.Sleep(1 * time.Second)
	}
}

func (suite *BlueGreenTestSuite) blueDnsMonitor(hostId, host string, stopChan <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	defer func() {
		slog.Debug("BlueDNS monitor completed", "hostId", hostId)
	}()

	time.Sleep(1 * time.Second)

	originalIP, err := net.LookupHost(host)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("BlueDNS @ %s: failed to lookup host: %w", hostId, err))
		return
	}

	slog.Debug("BlueDNS monitoring started", "hostId", hostId, "host", host, "originalIP", originalIP[0])

	results := suite.getResults(hostId)

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		time.Sleep(1 * time.Second)

		currentIP, err := net.LookupHost(host)
		if err != nil {
			slog.Debug("BlueDNS lookup error", "hostId", hostId, "error", err)
			results.DnsBlueError = err.Error()
			atomic.StoreInt64(&results.DnsBlueChangedTime, time.Now().UnixNano())
			return
		}

		if len(currentIP) > 0 && len(originalIP) > 0 && currentIP[0] != originalIP[0] {
			atomic.StoreInt64(&results.DnsBlueChangedTime, time.Now().UnixNano())
			slog.Debug("BlueDNS IP changed", "hostId", hostId, "host", host, "newIP", currentIP[0])
			return
		}
	}
}

func (suite *BlueGreenTestSuite) greenDnsMonitor(hostId, host string, stopChan <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	defer func() {
		slog.Debug("GreenDNS monitor completed", "hostId", hostId)
	}()

	time.Sleep(1 * time.Second)

	ip, err := net.LookupHost(host)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("GreenDNS @ %s: failed to lookup host: %w", hostId, err))
		return
	}

	slog.Debug("GreenDNS monitoring started", "hostId", hostId, "host", host, "ip", ip[0])

	results := suite.getResults(hostId)

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		time.Sleep(1 * time.Second)

		_, err := net.LookupHost(host)
		if err != nil {
			atomic.StoreInt64(&results.DnsGreenRemovedTime, time.Now().UnixNano())
			slog.Debug("GreenDNS removed", "hostId", hostId)
			return
		}
	}
}

func (suite *BlueGreenTestSuite) wrapperGreenConnectivityMonitor(hostId, host string, port int, dbName string,
	stopChan <-chan struct{}, wg *sync.WaitGroup, environment *test_utils.TestEnvironment) {
	defer wg.Done()

	var conn driver.Conn

	defer func() {
		suite.closeConnection(conn)
		slog.Debug("WrapperGreenConnectivity monitor completed", "hostId", hostId)
	}()

	props := suite.getWrapperConnectionProperties(environment)
	props[property_util.HOST.Name] = host
	props[property_util.PORT.Name] = strconv.Itoa(port)
	props[property_util.DATABASE.Name] = dbName

	dsn := test_utils.GetDsn(environment, props)

	var err error
	conn, err = suite.openConnectionWithRetry(dsn, environment)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("WrapperGreenConnectivity @ %s: failed to connect: %w", hostId, err))
		return
	}

	slog.Debug("WrapperGreenConnectivity connection opened", "hostId", hostId)

	time.Sleep(1 * time.Second)

	slog.Debug("WrapperGreenConnectivity starting connectivity monitoring", "hostId", hostId)

	results := suite.getResults(hostId)

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		startTime := time.Now().UnixNano()

		var holdTime time.Duration
		if awsConn, ok := conn.(*awsDriver.AwsWrapperConn); ok {
			if bgPlugin := awsConn.UnwrapPlugin(driver_infrastructure.BLUE_GREEN_PLUGIN_CODE); bgPlugin != nil {
				if blueGreenPlugin, ok := bgPlugin.(*bg.BlueGreenPlugin); ok {
					holdTime = blueGreenPlugin.GetHoldTimeNano()
				}
			}
		}

		err := execConn(conn, "SELECT 1")
		endTime := time.Now().UnixNano()

		timeHolder := TimeHolder{
			StartTime: startTime,
			EndTime:   endTime,
			HoldNano:  holdTime,
		}

		if err != nil {
			timeHolder.Error = err.Error()
			results.GreenWrapperExecuteTimesMutex.Lock()
			results.GreenWrapperExecuteTimes = append(results.GreenWrapperExecuteTimes, timeHolder)
			results.GreenWrapperExecuteTimesMutex.Unlock()

			// Check if connection is closed
			if pingConn(conn) != nil {
				atomic.StoreInt64(&results.WrapperGreenLostConnectionTime, time.Now().UnixNano())
				return
			}
		} else {
			results.GreenWrapperExecuteTimesMutex.Lock()
			results.GreenWrapperExecuteTimes = append(results.GreenWrapperExecuteTimes, timeHolder)
			results.GreenWrapperExecuteTimesMutex.Unlock()
		}

		time.Sleep(1 * time.Second)
	}
}

func (suite *BlueGreenTestSuite) greenIamConnectivityMonitor(
	hostId, threadPrefix, iamTokenHost string, port int, dbName string,
	stopChan <-chan struct{}, wg *sync.WaitGroup, environment *test_utils.TestEnvironment,
	resultQueue *[]TimeHolder, resultMutex *sync.Mutex,
	notifyOnFirstError, exitOnFirstSuccess bool) {
	defer wg.Done()

	defer func() {
		slog.Debug("DirectGreenIam monitor completed", "hostId", hostId, "prefix", threadPrefix)
	}()

	time.Sleep(1 * time.Second)

	slog.Debug("DirectGreenIam starting connectivity monitoring", "hostId", hostId, "prefix", threadPrefix, "iamTokenHost", iamTokenHost)

	results := suite.getResults(hostId)

	// Create AWS config for IAM token generation
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(environment.Info().Region),
	)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("DirectGreenIam%s @ %s: failed to load AWS config: %w", threadPrefix, hostId, err))
		return
	}

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		// Generate IAM token
		token, err := auth.BuildAuthToken(context.TODO(), iamTokenHost+":"+strconv.Itoa(port), environment.Info().Region, environment.Info().IamUsername, cfg.Credentials)
		if err != nil {
			suite.addUnhandledException(fmt.Errorf("DirectGreenIam%s @ %s: failed to generate IAM token: %w", threadPrefix, hostId, err))
			return
		}

		// Create connection properties for direct IAM without plugin
		props := map[string]string{
			property_util.HOST.Name:       iamTokenHost,
			property_util.PORT.Name:       strconv.Itoa(port),
			property_util.DATABASE.Name:   dbName,
			property_util.USER.Name:       environment.Info().IamUsername,
			property_util.PASSWORD.Name:   token,
			property_util.PLUGINS.Name:    "none",
			property_util.IAM_REGION.Name: environment.Info().Region,
		}
		props = addMySQLIamHandlingIfNecessary(environment, props)

		dsn := test_utils.GetDsn(environment, props)

		startTime := time.Now().UnixNano()

		db, err := suite.openConnectionWithRetry(dsn, environment)
		endTime := time.Now().UnixNano()

		timeHolder := TimeHolder{
			StartTime: startTime,
			EndTime:   endTime,
		}

		if err != nil {
			timeHolder.Error = err.Error()
			errString := strings.ToLower(err.Error())
			if notifyOnFirstError && (strings.Contains(errString, "access denied") || strings.Contains(errString, "pam")) {
				if atomic.CompareAndSwapInt64(&results.GreenNodeChangeNameTime, 0, time.Now().UnixNano()) {
					slog.Debug("DirectGreenIam first access denied error", "hostId", hostId, "prefix", threadPrefix)
				}

				resultMutex.Lock()
				*resultQueue = append(*resultQueue, timeHolder)
				resultMutex.Unlock()
				return
			}
		} else {
			if exitOnFirstSuccess {
				if atomic.CompareAndSwapInt64(&results.GreenNodeChangeNameTime, 0, time.Now().UnixNano()) {
					slog.Debug("DirectGreenIam successfully connected", "hostId", hostId, "prefix", threadPrefix)
				}

				resultMutex.Lock()
				*resultQueue = append(*resultQueue, timeHolder)
				resultMutex.Unlock()

				suite.closeConnection(db)
				return
			}
		}

		resultMutex.Lock()
		*resultQueue = append(*resultQueue, timeHolder)
		resultMutex.Unlock()

		suite.closeConnection(db)
		time.Sleep(1 * time.Second)
	}
}

func (suite *BlueGreenTestSuite) blueGreenSwitchoverTrigger(blueGreenId string, wg *sync.WaitGroup) {
	defer wg.Done()

	defer func() {
		slog.Debug("Switchover trigger completed")
	}()

	// Wait for all other goroutines to be ready
	slog.Info("Blue/Green switchover waiting 3 minutes for other goroutines", "blueGreenId", blueGreenId)
	time.Sleep(3 * time.Minute)

	// Set threads sync time
	threadsSyncTime := time.Now().UnixNano()
	suite.results.Range(func(key, value interface{}) bool {
		results := value.(*BlueGreenResults)
		atomic.StoreInt64(&results.ThreadsSyncTime, threadsSyncTime)
		return true
	})

	// Wait additional time before triggering switchover
	time.Sleep(30 * time.Second)
	slog.Info("Blue/Green switchover done waiting for other goroutines", "blueGreenId", blueGreenId)

	// Trigger switchover
	ctx := context.Background()
	err := suite.auroraUtil.SwitchoverBlueGreenDeployment(ctx, blueGreenId)
	if err != nil {
		fmt.Println("Blue/Green switchover command failed", "blueGreenId", blueGreenId, err.Error())
		suite.addUnhandledException(fmt.Errorf("failed to trigger Blue/Green switchover: %w", err))
		return
	}

	// Set trigger time
	bgTriggerTime := time.Now().UnixNano()
	suite.results.Range(func(key, value interface{}) bool {
		results := value.(*BlueGreenResults)
		atomic.StoreInt64(&results.BgTriggerTime, bgTriggerTime)
		return true
	})

	slog.Info("Blue/Green switchover triggered", "blueGreenId", blueGreenId)
}

func (suite *BlueGreenTestSuite) printMetrics() {
	// Get trigger time
	var bgTriggerTime int64
	suite.results.Range(func(key, value interface{}) bool {
		results := value.(*BlueGreenResults)
		if triggerTime := atomic.LoadInt64(&results.BgTriggerTime); triggerTime > 0 {
			bgTriggerTime = triggerTime
			return false // Stop iteration
		}
		return true
	})

	if bgTriggerTime == 0 {
		slog.Error("Cannot get bg_trigger_time")
		return
	}

	// Create main metrics table
	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{
		"Instance/endpoint",
		"Start time",
		"Threads sync",
		"Direct Blue conn dropped (idle)",
		"Direct Blue conn dropped (SELECT 1)",
		"Wrapper Blue conn dropped (idle)",
		"Wrapper Green conn dropped (SELECT 1)",
		"Blue DNS updated",
		"Green DNS removed",
		"Green node certificate change",
	})

	// Collect and sort entries
	type resultEntry struct {
		hostId  string
		results *BlueGreenResults
	}

	var entries []resultEntry
	suite.results.Range(func(key, value interface{}) bool {
		entries = append(entries, resultEntry{
			hostId:  key.(string),
			results: value.(*BlueGreenResults),
		})
		return true
	})

	// Sort entries: green instances first, then by name
	sort.Slice(entries, func(i, j int) bool {
		iIsGreen := utils.IsGreenInstance(entries[i].hostId + ".")
		jIsGreen := utils.IsGreenInstance(entries[j].hostId + ".")

		if iIsGreen != jIsGreen {
			return iIsGreen // Green instances first
		}

		iName := utils.RemoveGreenInstancePrefix(entries[i].hostId)
		jName := utils.RemoveGreenInstancePrefix(entries[j].hostId)
		return strings.ToLower(iName) < strings.ToLower(jName)
	})

	if len(entries) == 0 {
		_ = table.Append([]string{"No entries"})
	}

	for _, entry := range entries {
		results := entry.results
		startTime := (atomic.LoadInt64(&results.StartTime) - bgTriggerTime) / 1_000_000
		threadsSyncTime := (atomic.LoadInt64(&results.ThreadsSyncTime) - bgTriggerTime) / 1_000_000
		directBlueIdleLostTime := suite.getFormattedNanoTime(atomic.LoadInt64(&results.DirectBlueIdleLostConnectionTime), bgTriggerTime)
		directBlueLostTime := suite.getFormattedNanoTime(atomic.LoadInt64(&results.DirectBlueLostConnectionTime), bgTriggerTime)
		wrapperBlueIdleLostTime := suite.getFormattedNanoTime(atomic.LoadInt64(&results.WrapperBlueIdleLostConnectionTime), bgTriggerTime)
		wrapperGreenLostTime := suite.getFormattedNanoTime(atomic.LoadInt64(&results.WrapperGreenLostConnectionTime), bgTriggerTime)
		dnsBlueChangedTime := suite.getFormattedNanoTime(atomic.LoadInt64(&results.DnsBlueChangedTime), bgTriggerTime)
		dnsGreenRemovedTime := suite.getFormattedNanoTime(atomic.LoadInt64(&results.DnsGreenRemovedTime), bgTriggerTime)
		greenNodeChangeTime := suite.getFormattedNanoTime(atomic.LoadInt64(&results.GreenNodeChangeNameTime), bgTriggerTime)

		_ = table.Append([]string{
			entry.hostId,
			fmt.Sprintf("%d ms", startTime),
			fmt.Sprintf("%d ms", threadsSyncTime),
			directBlueIdleLostTime,
			directBlueLostTime,
			wrapperBlueIdleLostTime,
			wrapperGreenLostTime,
			dnsBlueChangedTime,
			dnsGreenRemovedTime,
			greenNodeChangeTime,
		})
	}

	fmt.Println("\nBlue/Green Switchover Metrics:")
	_ = table.Render()

	// Print detailed status times for each host
	for _, entry := range entries {
		suite.printNodeStatusTimes(entry.hostId, entry.results, bgTriggerTime)
	}

	// Print duration times for various connection types
	for _, entry := range entries {
		if len(entry.results.BlueWrapperConnectTimes) > 0 {
			suite.printDurationTimes(entry.hostId, "Wrapper connection time (ms) to Blue",
				entry.results.BlueWrapperConnectTimes, bgTriggerTime)
		}

		if len(entry.results.GreenDirectIamWithGreenNodeConnectTimes) > 0 {
			suite.printDurationTimes(entry.hostId, "Wrapper IAM (green token) connection time (ms) to Green",
				entry.results.GreenDirectIamWithGreenNodeConnectTimes, bgTriggerTime)
		}

		if len(entry.results.BlueWrapperExecuteTimes) > 0 {
			suite.printDurationTimes(entry.hostId, "Wrapper execution time (ms) to Blue",
				entry.results.BlueWrapperExecuteTimes, bgTriggerTime)
		}

		if len(entry.results.GreenWrapperExecuteTimes) > 0 {
			suite.printDurationTimes(entry.hostId, "Wrapper execution time (ms) to Green",
				entry.results.GreenWrapperExecuteTimes, bgTriggerTime)
		}
	}
}

func (suite *BlueGreenTestSuite) getFormattedNanoTime(timeNano, timeZeroNano int64) string {
	if timeNano == 0 {
		return "-"
	}
	return fmt.Sprintf("%d ms", (timeNano-timeZeroNano)/1_000_000)
}

func (suite *BlueGreenTestSuite) printNodeStatusTimes(hostId string, results *BlueGreenResults, timeZeroNano int64) {
	statusMap := make(map[string]int64)

	// Combine blue and green status times
	results.BlueStatusTime.Range(func(key, value interface{}) bool {
		statusMap[key.(string)] = value.(int64)
		return true
	})
	results.GreenStatusTime.Range(func(key, value interface{}) bool {
		statusMap[key.(string)] = value.(int64)
		return true
	})

	if len(statusMap) == 0 {
		return
	}

	// Sort statuses by time
	type statusEntry struct {
		status string
		time   int64
	}

	var statuses []statusEntry
	for status, statusTime := range statusMap {
		statuses = append(statuses, statusEntry{status: status, time: statusTime})
	}

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].time < statuses[j].time
	})

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{"Status", "SOURCE", "TARGET"})

	for _, statusEntry := range statuses {
		var sourceTime, targetTime string

		if value, exists := results.BlueStatusTime.Load(statusEntry.status); exists {
			sourceTime = fmt.Sprintf("%d ms", (value.(int64)-timeZeroNano)/1_000_000)
		}

		if value, exists := results.GreenStatusTime.Load(statusEntry.status); exists {
			targetTime = fmt.Sprintf("%d ms", (value.(int64)-timeZeroNano)/1_000_000)
		}

		_ = table.Append([]string{statusEntry.status, sourceTime, targetTime})
	}

	fmt.Printf("\n%s Status Times:\n", hostId)
	_ = table.Render()
}

func (suite *BlueGreenTestSuite) printDurationTimes(hostId, title string, times []TimeHolder, timeZeroNano int64) {
	if len(times) == 0 {
		return
	}

	durations := make([]int64, len(times))
	for i, t := range times {
		durations[i] = t.EndTime - t.StartTime
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	p99Index := int(math.Ceil(99.0/100.0*float64(len(durations)))) - 1
	if p99Index < 0 {
		p99Index = 0
	}
	p99 := durations[p99Index] / 1_000_000 // Convert to ms

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{"Connect at (ms)", "Connect time/duration (ms)", "Error"})

	_ = table.Append([]string{"p99", fmt.Sprintf("%d", p99), ""})

	first := times[0]
	_ = table.Append([]string{
		fmt.Sprintf("%d", (first.StartTime-timeZeroNano)/1_000_000),
		fmt.Sprintf("%d", (first.EndTime-first.StartTime)/1_000_000),
		suite.getFormattedError(first.Error),
	})

	for _, t := range times {
		duration := (t.EndTime - t.StartTime) / 1_000_000
		if duration > p99 {
			_ = table.Append([]string{
				fmt.Sprintf("%d", (t.StartTime-timeZeroNano)/1_000_000),
				fmt.Sprintf("%d", duration),
				suite.getFormattedError(t.Error),
			})
		}
	}

	last := times[len(times)-1]
	_ = table.Append([]string{
		fmt.Sprintf("%d", (last.StartTime-timeZeroNano)/1_000_000),
		fmt.Sprintf("%d", (last.EndTime-last.StartTime)/1_000_000),
		suite.getFormattedError(last.Error),
	})

	fmt.Printf("\n%s: %s\n", hostId, title)
	_ = table.Render()
}

func (suite *BlueGreenTestSuite) getFormattedError(err string) string {
	if err == "" {
		return ""
	}

	maxLen := 100
	if len(err) > maxLen {
		return strings.ReplaceAll(err[:maxLen], "\n", " ") + "..."
	}
	return strings.ReplaceAll(err, "\n", " ")
}

func (suite *BlueGreenTestSuite) logUnhandledExceptions() {
	suite.exceptionsMutex.Lock()
	defer suite.exceptionsMutex.Unlock()

	for _, err := range suite.unhandledExceptions {
		slog.Error("Unhandled exception", "error", err)
	}
}

func (suite *BlueGreenTestSuite) assertTest(t *testing.T) {
	// Get trigger time
	var bgTriggerTime int64
	suite.results.Range(func(key, value interface{}) bool {
		results := value.(*BlueGreenResults)
		if triggerTime := atomic.LoadInt64(&results.BgTriggerTime); triggerTime > 0 {
			bgTriggerTime = triggerTime
			return false
		}
		return true
	})

	require.NotZero(t, bgTriggerTime, "Cannot get bg_trigger_time")

	// Find max green node change time
	var maxGreenNodeChangeTime int64
	suite.results.Range(func(key, value interface{}) bool {
		results := value.(*BlueGreenResults)
		if changeTime := atomic.LoadInt64(&results.GreenNodeChangeNameTime); changeTime > 0 {
			relativeTime := (changeTime - bgTriggerTime) / 1_000_000
			if relativeTime > maxGreenNodeChangeTime {
				maxGreenNodeChangeTime = relativeTime
			}
		}
		return true
	})

	slog.Info("Max green node change time", "time_ms", maxGreenNodeChangeTime)

	// Find switchover complete time
	var switchoverCompleteTime int64
	suite.results.Range(func(key, value interface{}) bool {
		results := value.(*BlueGreenResults)
		if value, exists := results.GreenStatusTime.Load("SWITCHOVER_COMPLETED"); exists {
			relativeTime := (value.(int64) - bgTriggerTime) / 1_000_000
			if relativeTime > switchoverCompleteTime {
				switchoverCompleteTime = relativeTime
			}
		}
		return true
	})

	slog.Info("Switchover complete time", "time_ms", switchoverCompleteTime)

	assert.NotZero(t, switchoverCompleteTime, "BG switchover hasn't completed")
	assert.GreaterOrEqual(t, switchoverCompleteTime, maxGreenNodeChangeTime,
		"Green node changed name after SWITCHOVER_COMPLETED")
}

func (suite *BlueGreenTestSuite) wrapperBlueIdleConnectivityMonitor(hostId, host string, port int, dbName string,
	stopChan <-chan struct{}, wg *sync.WaitGroup, environment *test_utils.TestEnvironment) {
	defer wg.Done()

	var conn driver.Conn

	defer func() {
		suite.closeConnection(conn)
		slog.Debug("WrapperBlueIdle monitor completed", "hostId", hostId)
	}()

	props := suite.getWrapperConnectionProperties(environment)
	props[property_util.HOST.Name] = host
	props[property_util.PORT.Name] = strconv.Itoa(port)
	props[property_util.DATABASE.Name] = dbName

	dsn := test_utils.GetDsn(environment, props)

	var err error
	conn, err = suite.openConnectionWithRetry(dsn, environment)
	if err != nil {
		suite.addUnhandledException(fmt.Errorf("WrapperBlueIdle @ %s: failed to connect: %w", hostId, err))
		return
	}

	slog.Debug("WrapperBlueIdle connection opened", "hostId", hostId)

	time.Sleep(1 * time.Second)

	slog.Debug("WrapperBlueIdle starting connectivity monitoring", "hostId", hostId)

	results := suite.getResults(hostId)

	for {
		select {
		case <-stopChan:
			return
		default:
		}

		err := pingConn(conn)
		if err != nil {
			slog.Debug("WrapperBlueIdle ping failed", "hostId", hostId, "error", err)
			atomic.StoreInt64(&results.WrapperBlueIdleLostConnectionTime, time.Now().UnixNano())
			return
		}

		time.Sleep(1 * time.Second)
	}
}

func pingConn(conn driver.Conn) error {
	pinger, ok := conn.(driver.Pinger)
	if ok {
		return pinger.Ping(context.Background())
	}
	return errors.New("conn does not implement Pinger")
}

func execConn(conn driver.Conn, query string) error {
	execer, ok := conn.(driver.ExecerContext)
	if ok {
		_, err := execer.ExecContext(context.Background(), query, nil)
		return err
	}
	return errors.New("conn does not implement ExecerContext")
}

func queryConn(conn driver.Conn, query string) (driver.Rows, error) {
	queryer, ok := conn.(driver.QueryerContext)
	if ok {
		return queryer.QueryContext(context.Background(), query, nil)
	}
	return nil, errors.New("conn does not implement QueryerContext")
}
