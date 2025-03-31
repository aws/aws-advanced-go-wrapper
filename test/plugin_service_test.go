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
	"awssql/driver_infrastructure"
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

func beforePluginServiceTests() (*plugin_helpers.PluginServiceImpl, *MockPluginManager, *host_info_util.HostInfoBuilder, error) {
	props := map[string]string{"protocol": "postgresql"}
	mockTargetDriver := &MockTargetDriver{}
	mockPluginManager := &MockPluginManager{plugin_helpers.NewPluginManagerImpl(mockTargetDriver, props), nil}
	target, err := plugin_helpers.NewPluginServiceImpl(mockPluginManager, &driver_infrastructure.PgxDriverDialect{}, props, pgTestDsn)
	return target, mockPluginManager, host_info_util.NewHostInfoBuilder(), err
}

func TestGetCurrentHostInfo(t *testing.T) {
	// If currentHostInfo is set returns it.
	target, _, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)
	err = target.SetCurrentConnection(MockDriverConn{}, hostInfoBuilder.SetHost("currentHost").SetRole(host_info_util.READER).
		SetAvailability(host_info_util.AVAILABLE).Build(), nil)
	assert.Nil(t, err)

	hostInfo, err := target.GetCurrentHostInfo()
	assert.Nil(t, err)
	assert.Equal(t, hostInfo.Host, "currentHost")

	// Given an initialHostInfo but no currentHostInfo returns that.
	target, _, _, err = beforePluginServiceTests()
	assert.Nil(t, err)
	target.SetInitialConnectionHostInfo(hostInfoBuilder.SetHost("initialHost").SetRole(host_info_util.READER).SetAvailability(host_info_util.AVAILABLE).Build())

	hostInfo, err = target.GetCurrentHostInfo()
	assert.Nil(t, err)
	assert.Equal(t, hostInfo.Host, "initialHost")

	// Given readers and writers only in AllHosts, returns the writer.
	target, _, _, err = beforePluginServiceTests()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{
		hostInfoBuilder.SetHost("reader").SetRole(host_info_util.READER).SetAvailability(host_info_util.AVAILABLE).Build(),
		hostInfoBuilder.SetHost("writer").SetRole(host_info_util.WRITER).SetAvailability(host_info_util.AVAILABLE).Build()}
	hostInfo, err = target.GetCurrentHostInfo()
	assert.Equal(t, err, nil)
	assert.Equal(t, hostInfo.Host, "writer")

	// Given only readers in AllHosts, returns the first reader.
	target, _, _, err = beforePluginServiceTests()
	assert.Nil(t, err)
	target.AllHosts = []*host_info_util.HostInfo{hostInfoBuilder.SetHost("singleReader").SetRole(host_info_util.READER).SetAvailability(host_info_util.AVAILABLE).Build()}
	hostInfo, err = target.GetCurrentHostInfo()
	assert.Equal(t, err, nil)
	assert.Equal(t, hostInfo.Host, "singleReader")

	// No hosts given at all, should return an error.
	target, _, _, err = beforePluginServiceTests()
	assert.Nil(t, err)
	_, err = target.GetCurrentHostInfo()
	assert.NotEqual(t, err, nil)
}

func TestSetCurrentConnection(t *testing.T) {
	target, _, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)

	_, oldConnectionImplementsQueryer := target.GetCurrentConnection().(driver.QueryerContext)

	newConnection := driver.Conn(MockConn{})
	_, newConnectionImplementsQueryer := (newConnection).(driver.QueryerContext)
	newHostInfo := hostInfoBuilder.SetHost("new-host").SetPort(1000).SetRole(host_info_util.WRITER).SetAvailability(host_info_util.AVAILABLE).Build()
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
	target, mockPluginManager, _, err := beforePluginServiceTests()
	assert.Nil(t, err)

	target.SetHostListProvider(&MockHostListProvider{})

	err = target.RefreshHostList(MockConn{})

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
	target, mockPluginManager, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)

	target.SetHostListProvider(&MockHostListProvider{})

	target.AllHosts = []*host_info_util.HostInfo{hostInfoBuilder.SetHost("hostA").Build(), hostInfoBuilder.SetHost("hostB").Build()}

	assert.Equal(t, 2, len(target.GetHosts()))

	err = target.RefreshHostList(MockConn{})

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
	target, mockPluginManager, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)

	target.SetHostListProvider(&MockHostListProvider{})

	target.AllHosts = []*host_info_util.HostInfo{hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).Build()}

	assert.Equal(t, 1, len(target.GetHosts()))
	assert.Equal(t, "hostA", target.GetHosts()[0].Host)
	assert.Equal(t, host_info_util.READER, target.GetHosts()[0].Role)

	err = target.RefreshHostList(MockConn{})

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
	target, mockPluginManager, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)

	target.AllHosts = []*host_info_util.HostInfo{hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()}
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
	target, mockPluginManager, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)

	target.AllHosts = []*host_info_util.HostInfo{hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.AVAILABLE).Build()}
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
	target, mockPluginManager, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)

	hostA := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	hostA.AddAlias("ip-10-10-10-10")
	hostA.AddAlias("hostA.custom.domain.com")
	hostB := hostInfoBuilder.SetHost("hostB").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
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
	target, mockPluginManager, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)

	hostA := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	hostA.AddAlias("ip-10-10-10-10")
	hostA.AddAlias("hostA.custom.domain.com")
	hostB := hostInfoBuilder.SetHost("hostB").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
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
	target, _, _, err := beforePluginServiceTests()
	assert.Nil(t, err)
	target.SetHostListProvider(&MockHostListProvider{})

	hostInfo, err := target.IdentifyConnection(MockDriverConn{})
	assert.Nil(t, err)
	assert.Equal(t, "hostA", hostInfo.Host)
}

func TestFillAliases(t *testing.T) {
	target, _, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)
	target.SetHostListProvider(&MockHostListProvider{})

	hostA := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()

	assert.Equal(t, 0, len(hostA.Aliases))
	target.FillAliases(MockDriverConn{}, hostA)
	assert.Equal(t, 1, len(hostA.Aliases))
}

func TestFillAliasesNonEmptyAliases(t *testing.T) {
	target, _, hostInfoBuilder, err := beforePluginServiceTests()
	assert.Nil(t, err)

	hostA := hostInfoBuilder.SetHost("hostA").SetRole(host_info_util.READER).SetAvailability(host_info_util.UNAVAILABLE).Build()
	hostA.AddAlias("hostA.custom.domain.com")

	assert.Equal(t, 1, len(hostA.Aliases))
	target.FillAliases(MockDriverConn{}, hostA)
	assert.Equal(t, 1, len(hostA.Aliases))
}
