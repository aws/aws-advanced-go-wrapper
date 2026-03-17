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
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/stretchr/testify/assert"
)

func TestHighestWeightHostSelectorGetHost(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	host3, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(15).Build()
	host4, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(3).Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2, host3, host4}

	hostSelector := driver_infrastructure.HighestWeightHostSelector{}
	selectedHost, err := hostSelector.GetHost(hostList, host_info_util.WRITER, nil)

	assert.Nil(t, err)
	assert.Equal(t, host3, selectedHost)
}

func TestHighestWeightHostSelectorGetHostGivenSingleHost(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	hostList := []*host_info_util.HostInfo{host0}

	hostSelector := driver_infrastructure.HighestWeightHostSelector{}
	selectedHost, err := hostSelector.GetHost(hostList, host_info_util.WRITER, nil)

	assert.Nil(t, err)
	assert.Equal(t, host0, selectedHost)
}

func TestHighestWeightHostSelectorGetHostGivenNoHosts(t *testing.T) {
	hostList := []*host_info_util.HostInfo{}
	hostSelector := driver_infrastructure.HighestWeightHostSelector{}
	_, err := hostSelector.GetHost(hostList, host_info_util.WRITER, nil)
	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HighestWeightHostSelector.noHostsMatchingRole", host_info_util.WRITER), err.Error())
}

func TestHighestWeightHostSelectorGetHostGivenNoMatchingRoles(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).Build()
	host3, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(15).Build()
	host4, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(3).Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2, host3, host4}

	hostSelector := driver_infrastructure.HighestWeightHostSelector{}
	_, err := hostSelector.GetHost(hostList, host_info_util.READER, nil)
	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HighestWeightHostSelector.noHostsMatchingRole", host_info_util.READER), err.Error())
}

func TestHighestWeightHostSelectorGetHostGivenNoAvailableHosts(t *testing.T) {
	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").SetWeight(10).SetAvailability(host_info_util.UNAVAILABLE).Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").SetWeight(9).SetAvailability(host_info_util.UNAVAILABLE).Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(8).SetAvailability(host_info_util.UNAVAILABLE).Build()
	host3, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(15).SetAvailability(host_info_util.UNAVAILABLE).Build()
	host4, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").SetWeight(3).SetAvailability(host_info_util.UNAVAILABLE).Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2, host3, host4}

	hostSelector := driver_infrastructure.HighestWeightHostSelector{}
	_, err := hostSelector.GetHost(hostList, host_info_util.WRITER, nil)
	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HighestWeightHostSelector.noHostsMatchingRole", host_info_util.WRITER), err.Error())
}

// Verifies that the selector only considers eligible hosts (matching role and available),
// ignoring ineligible hosts even if they have a higher weight.
func TestHighestWeightHostSelectorIgnoresIneligibleHosts(t *testing.T) {
	// Unavailable host has the highest weight — should be ignored.
	unavailableHost, _ := host_info_util.NewHostInfoBuilder().SetHost("unavailable").SetWeight(100).SetAvailability(host_info_util.UNAVAILABLE).Build()
	// Reader host has a high weight but wrong role — should be ignored when selecting writers.
	readerHost, _ := host_info_util.NewHostInfoBuilder().SetHost("reader").SetWeight(50).SetRole(host_info_util.READER).Build()
	// Two eligible writer hosts — the one with weight 10 should win.
	writerLow, _ := host_info_util.NewHostInfoBuilder().SetHost("writer-low").SetWeight(5).Build()
	writerHigh, _ := host_info_util.NewHostInfoBuilder().SetHost("writer-high").SetWeight(10).Build()
	hostList := []*host_info_util.HostInfo{unavailableHost, readerHost, writerLow, writerHigh}

	hostSelector := driver_infrastructure.HighestWeightHostSelector{}
	selectedHost, err := hostSelector.GetHost(hostList, host_info_util.WRITER, nil)

	assert.Nil(t, err)
	assert.Equal(t, writerHigh, selectedHost)
}
