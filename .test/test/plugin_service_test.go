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
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/services"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	pgx_driver "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
	"github.com/stretchr/testify/assert"
)

func beforePluginServiceTests() (*plugin_helpers.PluginServiceImpl, *MockPluginManager, *host_info_util.HostInfoBuilder, *services.FullServicesContainer, error) {
	props := MakeMapFromKeysAndVals("protocol", "postgresql")
	mockTargetDriver := &MockTargetDriver{}
	telemetryFactory, _ := telemetry.NewDefaultTelemetryFactory(props)
	container := &services.FullServicesContainer{
		Telemetry: telemetryFactory,
	}
	realPluginManager := plugin_helpers.NewPluginManagerImpl(mockTargetDriver, container, props)
	mockPluginManager := &MockPluginManager{realPluginManager, nil, nil}
	container.PluginManager = mockPluginManager
	target, err := plugin_helpers.NewPluginServiceImpl(container, pgx_driver.NewPgxDriverDialect(), props, pgTestDsn)
	impl, _ := target.(*plugin_helpers.PluginServiceImpl)
	return impl, mockPluginManager, host_info_util.NewHostInfoBuilder(), container, err
}

func TestGetCurrentHostInfo(t *testing.T) {
	// If currentHostInfo is set returns it.
	target, _, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)
	currentHost, err := hostInfoBuilder.SetHost("currentHost").SetRole(host_info_util.READER).
		SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	err = target.SetCurrentConnection(MockDriverConn{}, currentHost, nil)
	assert.Nil(t, err)

	hostInfo, err := target.GetCurrentHostInfo()
	assert.Nil(t, err)
	assert.Equal(t, hostInfo.Host, "currentHost")

	// Given an initialHostInfo but no currentHostInfo returns that.
	target, _, _, _, err = beforePluginServiceTests()
	assert.Nil(t, err)
	initialHost, err := hostInfoBuilder.SetHost("initialHost").SetRole(host_info_util.READER).
		SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	target.SetInitialConnectionHostInfo(initialHost)

	hostInfo, err = target.GetCurrentHostInfo()
	assert.Nil(t, err)
	assert.Equal(t, hostInfo.Host, "initialHost")

	// Given readers and writers only in AllHosts, returns the writer.
	target, _, _, _, err = beforePluginServiceTests()
	assert.Nil(t, err)
	reader, err := hostInfoBuilder.SetHost("reader").SetRole(host_info_util.READER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	writer, err := hostInfoBuilder.SetHost("writer").SetRole(host_info_util.WRITER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{
		reader,
		writer}
	hostInfo, err = target.GetCurrentHostInfo()
	assert.Equal(t, err, nil)
	assert.Equal(t, hostInfo.Host, "writer")

	// Given only readers in AllHosts, returns the first reader.
	target, _, _, _, err = beforePluginServiceTests()
	assert.Nil(t, err)
	singleReader, err := hostInfoBuilder.SetHost("singleReader").SetRole(host_info_util.READER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{singleReader}
	hostInfo, err = target.GetCurrentHostInfo()
	assert.Equal(t, err, nil)
	assert.Equal(t, hostInfo.Host, "singleReader")

	// No hosts given at all, should return an error.
	target, _, _, _, err = beforePluginServiceTests()
	assert.Nil(t, err)
	_, err = target.GetCurrentHostInfo()
	assert.NotEqual(t, err, nil)
}

func TestSetCurrentConnection(t *testing.T) {
	target, _, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	_, oldConnectionImplementsQueryer := target.GetCurrentConnection().(driver.QueryerContext)

	newConnection := driver.Conn(&MockConn{})
	_, newConnectionImplementsQueryer := (newConnection).(driver.QueryerContext)
	newHostInfo, err := hostInfoBuilder.SetHost("new-host").SetPort(1000).SetRole(host_info_util.WRITER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	err = target.SetCurrentConnection(newConnection, newHostInfo, nil)
	assert.Nil(t, err)

	currentHostInfo, _ := target.GetCurrentHostInfo()

	_, currentConnectionImplementsQueryer := target.GetCurrentConnection().(driver.QueryerContext)

	assert.NotEqual(t, oldConnectionImplementsQueryer, currentConnectionImplementsQueryer)
	assert.Equal(t, newConnectionImplementsQueryer, currentConnectionImplementsQueryer)
	assert.Equal(t, "new-host", currentHostInfo.Host)
	assert.Equal(t, 1000, currentHostInfo.Port)
	assert.Equal(t, host_info_util.WRITER, currentHostInfo.Role)
	assert.Equal(t, host_info_util.AVAILABLE, currentHostInfo.Availability)
}

func TestSetHostListAdd(t *testing.T) {
	target, mockPluginManager, _, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	target.SetHostListProvider(&MockHostListProvider{})

	err = target.RefreshHostList(&MockConn{})

	assert.Nil(t, err)
	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, "hostA", target.GetHosts()[0].Host)

	assert.Equal(t, 1, len(mockPluginManager.Changes))
	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 1, len(hostAChangeMap))
	_, addedHostA := hostAChangeMap[driver_infrastructure.HOST_ADDED]
	assert.True(t, addedHostA)
}

func TestSetHostListDelete(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	target.SetHostListProvider(&MockHostListProvider{})

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.WRITER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	hostB, err := hostInfoBuilder.SetHost("hostB").SetRole(host_info_util.WRITER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{hostA, hostB}

	assert.Equal(t, 2, len(target.GetHosts()))

	err = target.RefreshHostList(&MockConn{})

	assert.Nil(t, err)
	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, "hostA", target.GetHosts()[0].Host)

	assert.Equal(t, 1, len(mockPluginManager.Changes))
	hostBChangeMap, hostBInChangeMap := mockPluginManager.Changes["hostB"]
	assert.True(t, hostBInChangeMap)
	assert.Equal(t, 1, len(hostBChangeMap))
	_, deletedHostB := hostBChangeMap[driver_infrastructure.HOST_DELETED]
	assert.True(t, deletedHostB)
}

func TestSetHostListChange(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	target.SetHostListProvider(&MockHostListProvider{})

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{hostA}

	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, "hostA", target.GetHosts()[0].Host)
	assert.Equal(t, host_info_util.READER, target.GetHosts()[0].Role)

	err = target.RefreshHostList(&MockConn{})

	assert.Nil(t, err)
	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, "hostA", target.GetHosts()[0].Host)
	assert.Equal(t, host_info_util.WRITER, target.GetHosts()[0].Role)

	assert.Equal(t, 1, len(mockPluginManager.Changes))
	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, promotedToWriterHostA := hostAChangeMap[driver_infrastructure.PROMOTED_TO_WRITER]
	assert.True(t, changedHostA)
	assert.True(t, promotedToWriterHostA)
}

func TestHostAvailabilityWentUp(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.WRITER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{hostA}
	target.SetAvailability(map[string]bool{"hostA": true}, host_info_util.AVAILABLE)

	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, host_info_util.AVAILABLE, target.GetHosts()[0].Availability)

	assert.Equal(t, 1, len(mockPluginManager.Changes))
	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentUpHostA := hostAChangeMap[driver_infrastructure.WENT_UP]
	assert.True(t, changedHostA)
	assert.True(t, wentUpHostA)
}

func TestHostAvailabilityWentDown(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.WRITER).SetAvailability(host_info_util.AVAILABLE).Build()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{hostA}
	target.SetAvailability(map[string]bool{"hostA": true}, host_info_util.UNAVAILABLE)

	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, host_info_util.UNAVAILABLE, target.GetHosts()[0].Availability)

	assert.Equal(t, 1, len(mockPluginManager.Changes))
	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentDownHostA := hostAChangeMap[driver_infrastructure.WENT_DOWN]
	assert.True(t, changedHostA)
	assert.True(t, wentDownHostA)
}

func TestHostAvailabilityWentUpByAlias(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostA.AddAlias("ip-10-10-10-10")
	hostA.AddAlias("hostA.custom.domain.com")
	hostB, err := hostInfoBuilder.SetHost("hostB").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostB.AddAlias("ip-10-10-10-10")
	hostB.AddAlias("hostB.custom.domain.com")

	target.AllHosts = []*host_info_util.HostInfo{hostA, hostB}
	target.SetAvailability(map[string]bool{"hostA.custom.domain.com": true}, host_info_util.AVAILABLE)

	assert.Equal(t, 2, len(target.GetHosts()))
	assert.Equal(t, host_info_util.AVAILABLE, target.GetHosts()[0].Availability)
	assert.Equal(t, host_info_util.UNAVAILABLE, target.GetHosts()[1].Availability)

	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentUpHostA := hostAChangeMap[driver_infrastructure.WENT_UP]
	assert.True(t, changedHostA)
	assert.True(t, wentUpHostA)

	_, hostBInChangeMap := mockPluginManager.Changes["hostB"]
	assert.False(t, hostBInChangeMap)
}

func TestHostAvailabilityWentUpMultipleHostsByAlias(t *testing.T) {
	target, mockPluginManager, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostA.AddAlias("ip-10-10-10-10")
	hostA.AddAlias("hostA.custom.domain.com")
	hostB, err := hostInfoBuilder.SetHost("hostB").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostB.AddAlias("ip-10-10-10-10")
	hostB.AddAlias("hostB.custom.domain.com")

	target.AllHosts = []*host_info_util.HostInfo{hostA, hostB}
	target.SetAvailability(map[string]bool{"ip-10-10-10-10": true}, host_info_util.AVAILABLE)

	assert.Equal(t, 2, len(target.GetHosts()))
	assert.Equal(t, host_info_util.AVAILABLE, target.GetHosts()[0].Availability)
	assert.Equal(t, host_info_util.AVAILABLE, target.GetHosts()[1].Availability)

	hostAChangeMap, hostAInChangeMap := mockPluginManager.Changes["hostA"]
	assert.True(t, hostAInChangeMap)
	assert.Equal(t, 2, len(hostAChangeMap))
	_, changedHostA := hostAChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentUpHostA := hostAChangeMap[driver_infrastructure.WENT_UP]
	assert.True(t, changedHostA)
	assert.True(t, wentUpHostA)

	hostBChangeMap, hostBInChangeMap := mockPluginManager.Changes["hostB"]
	assert.True(t, hostBInChangeMap)
	assert.Equal(t, 2, len(hostBChangeMap))
	_, changedHostB := hostBChangeMap[driver_infrastructure.HOST_CHANGED]
	_, wentUpHostB := hostAChangeMap[driver_infrastructure.WENT_UP]
	assert.True(t, changedHostB)
	assert.True(t, wentUpHostB)
}

func TestIdentifyConnection(t *testing.T) {
	target, _, _, _, err := beforePluginServiceTests()
	assert.Nil(t, err)
	target.SetHostListProvider(&MockHostListProvider{})

	hostInfo, err := target.IdentifyConnection(MockDriverConn{})
	assert.Nil(t, err)
	assert.Equal(t, "hostA", hostInfo.Host)
}

func TestFillAliases(t *testing.T) {
	target, _, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)
	target.SetHostListProvider(&MockHostListProvider{})

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()

	assert.Nil(t, err)
	assert.Equal(t, 0, len(hostA.Aliases))
	target.FillAliases(MockDriverConn{}, hostA)
	assert.Equal(t, 1, len(hostA.Aliases))
}

func TestFillAliasesNonEmptyAliases(t *testing.T) {
	target, _, hostInfoBuilder, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	hostA, err := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	assert.Nil(t, err)
	hostA.AddAlias("hostA.custom.domain.com")

	assert.Equal(t, 1, len(hostA.Aliases))
	target.FillAliases(MockDriverConn{}, hostA)
	assert.Equal(t, 1, len(hostA.Aliases))
}

func TestGetStatusEmptyCache(t *testing.T) {
	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)
	defer storage.Stop()

	cacheKey := "test-id::BlueGreenStatus"
	status, found := driver_infrastructure.BlueGreenStatusStorageType.Get(storage, cacheKey)
	assert.False(t, found, "Should return false when status not found in cache")
	assert.Nil(t, status, "Should return nil when not found")
}

func TestGetSetStatus(t *testing.T) {
	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)
	defer storage.Stop()

	bgStatus := driver_infrastructure.NewBgStatus(
		"test-id",
		driver_infrastructure.IN_PROGRESS,
		[]driver_infrastructure.ConnectRouting{},
		[]driver_infrastructure.ExecuteRouting{},
		utils.NewRWMap[string, driver_infrastructure.BlueGreenRole](),
		utils.NewRWMap[string, utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]](),
	)

	cacheKey := "test-bg::BlueGreenStatus"
	driver_infrastructure.BlueGreenStatusStorageType.Set(storage, cacheKey, &bgStatus)

	// Try to retrieve with the exact key
	retrievedStatus, found := driver_infrastructure.BlueGreenStatusStorageType.Get(storage, cacheKey)
	assert.True(t, found, "Should find status with exact key")
	assert.NotNil(t, retrievedStatus, "Status should not be nil")
	assert.Equal(t, driver_infrastructure.IN_PROGRESS, retrievedStatus.GetCurrentPhase(), "Should retrieve correct phase")

	// Verify that normalized keys also work (matching bg_status_provider behavior with lowercase+trim)
	testCases := []struct {
		input    string
		expected string
	}{
		{"test-bg", "test-bg"},
		{"TEST-BG", "test-bg"},
		{"Test-Bg", "test-bg"},
		{"TeSt-Bg", "test-bg"},
		{"  test-bg", "test-bg"},
		{"test-bg  ", "test-bg"},
		{"  test-bg  ", "test-bg"},
		{"\ttest-bg\t", "test-bg"},
		{"\ntest-bg\n", "test-bg"},
	}

	for _, tc := range testCases {
		normalizedKey := strings.ToLower(strings.TrimSpace(tc.input)) + "::BlueGreenStatus"
		retrievedStatus, found := driver_infrastructure.BlueGreenStatusStorageType.Get(storage, normalizedKey)
		assert.True(t, found, "Should find status for normalized ID: %s -> %s", tc.input, normalizedKey)
		assert.NotNil(t, retrievedStatus, "Status should not be nil for ID: %s", tc.input)
		assert.Equal(t, driver_infrastructure.IN_PROGRESS, retrievedStatus.GetCurrentPhase(), "Should retrieve correct phase for ID: %s", tc.input)
	}
}

func TestSetStatusUpdateExistingStatus(t *testing.T) {
	storage := services.NewExpiringStorage(5*time.Minute, nil)
	driver_infrastructure.RegisterDefaultStorageTypes(storage)
	defer storage.Stop()

	testId := "deployment-update-test"
	cacheKey := testId + "::BlueGreenStatus"
	connectRoutings := []driver_infrastructure.ConnectRouting{}
	executeRoutings := []driver_infrastructure.ExecuteRouting{}
	roleByHost := utils.NewRWMap[string, driver_infrastructure.BlueGreenRole]()
	correspondingHosts := utils.NewRWMap[string, utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]()

	initialStatus := driver_infrastructure.NewBgStatus(testId, driver_infrastructure.CREATED, connectRoutings, executeRoutings, roleByHost, correspondingHosts)
	driver_infrastructure.BlueGreenStatusStorageType.Set(storage, cacheKey, &initialStatus)

	retrievedStatus, found := driver_infrastructure.BlueGreenStatusStorageType.Get(storage, cacheKey)
	assert.True(t, found, "Should find initial status")
	assert.Equal(t, driver_infrastructure.CREATED, retrievedStatus.GetCurrentPhase(), "Should have initial phase")

	updatedStatus := driver_infrastructure.NewBgStatus(testId, driver_infrastructure.COMPLETED, connectRoutings, executeRoutings, roleByHost, correspondingHosts)
	driver_infrastructure.BlueGreenStatusStorageType.Set(storage, cacheKey, &updatedStatus)

	retrievedStatus, found = driver_infrastructure.BlueGreenStatusStorageType.Get(storage, cacheKey)
	assert.True(t, found, "Should find updated status")
	assert.Equal(t, driver_infrastructure.COMPLETED, retrievedStatus.GetCurrentPhase(), "Should have updated phase")

	driver_infrastructure.BlueGreenStatusStorageType.Remove(storage, cacheKey)

	_, found = driver_infrastructure.BlueGreenStatusStorageType.Get(storage, cacheKey)
	assert.False(t, found, "Should remove status")
}
