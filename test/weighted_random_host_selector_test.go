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
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/property_util"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWeightedRandomHostSelectorGetHost(t *testing.T) {
	props := make(map[string]string)

	hostSelector := driver_infrastructure.WeightedRandomHostSelector{}

	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	hostSelector.SetRandomNumberFunc(func(int) int { return 0 })
	actualHost0, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost0, host0)

	hostSelector.SetRandomNumberFunc(func(int) int { return 1 })
	actualHost1, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost1, host1)

	hostSelector.SetRandomNumberFunc(func(int) int { return 2 })
	actualHost2, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost2, host2)
}

func TestWeightedRandomHostSelectorGetHostBySettingWeights(t *testing.T) {
	props := make(map[string]string)

	hostSelector := driver_infrastructure.WeightedRandomHostSelector{}

	hostWeights := map[string]int{
		"host0": 1,
		"host1": 2,
		"host2": 3,
	}
	hostSelector.SetHostWeights(hostWeights)

	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	hostSelector.SetRandomNumberFunc(func(int) int { return 0 })
	actualHost0, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost0, host0)

	hostSelector.SetRandomNumberFunc(func(int) int { return 1 })
	actualHost1, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost1, host1)

	hostSelector.SetRandomNumberFunc(func(int) int { return 2 })
	actualHost2, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost2, host1)

	hostSelector.SetRandomNumberFunc(func(int) int { return 3 })
	actualHost3, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost3, host2)

	hostSelector.SetRandomNumberFunc(func(int) int { return 4 })
	actualHost4, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost4, host2)

	hostSelector.SetRandomNumberFunc(func(int) int { return 5 })
	actualHost5, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost5, host2)
}

func TestWeightedRandomHostSelectorGetHostByProps(t *testing.T) {
	props := map[string]string{
		property_util.WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.Name: "host0:3,host1:2,host2:01",
	}

	hostSelector := driver_infrastructure.WeightedRandomHostSelector{}

	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	hostSelector.SetRandomNumberFunc(func(int) int { return 0 })
	actualHost0, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost0, host0)

	hostSelector.SetRandomNumberFunc(func(int) int { return 1 })
	actualHost1, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost1, host0)

	hostSelector.SetRandomNumberFunc(func(int) int { return 2 })
	actualHost2, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost2, host0)

	hostSelector.SetRandomNumberFunc(func(int) int { return 3 })
	actualHost3, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost3, host1)

	hostSelector.SetRandomNumberFunc(func(int) int { return 4 })
	actualHost4, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost4, host1)

	hostSelector.SetRandomNumberFunc(func(int) int { return 5 })
	actualHost5, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.Nil(t, err)
	assert.Equal(t, actualHost5, host2)
}

func TestWeightedRandomHostSelectorGetHostByInvalidWeightsReturnsError(t *testing.T) {
	hostSelector := driver_infrastructure.WeightedRandomHostSelector{}

	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	propsWeightOfZero := map[string]string{
		property_util.WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.Name: "host0:3,host1:2,host2:0",
	}

	_, err := hostSelector.GetHost(hostList, host_info_util.WRITER, propsWeightOfZero)
	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HostSelector.invalidHostWeightPairs"), err.Error())

	propsNegativeWeight := map[string]string{
		property_util.WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.Name: "host0:3,host1:-1,host2:2",
	}

	_, err = hostSelector.GetHost(hostList, host_info_util.WRITER, propsNegativeWeight)
	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HostSelector.invalidHostWeightPairs"), err.Error())
}

func TestWeightedRandomHostSelectorGetHostByInvalidPropsReturnsError(t *testing.T) {
	props := map[string]string{
		property_util.WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.Name: "someinvalidString",
	}

	hostSelector := driver_infrastructure.WeightedRandomHostSelector{}

	host0, _ := host_info_util.NewHostInfoBuilder().SetHost("host0").Build()
	host1, _ := host_info_util.NewHostInfoBuilder().SetHost("host1").Build()
	host2, _ := host_info_util.NewHostInfoBuilder().SetHost("host2").Build()
	hostList := []*host_info_util.HostInfo{host0, host1, host2}

	_, err := hostSelector.GetHost(hostList, host_info_util.WRITER, props)
	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HostSelector.invalidHostWeightPairs"), err.Error())
}

func TestIsInNumberRange(t *testing.T) {
	assert.True(t, driver_infrastructure.NumberRange{Start: 1, End: 1}.IsInNumberRange(1))
	assert.False(t, driver_infrastructure.NumberRange{Start: 1, End: 1}.IsInNumberRange(0))
	assert.False(t, driver_infrastructure.NumberRange{Start: 1, End: 1}.IsInNumberRange(2))
	assert.False(t, driver_infrastructure.NumberRange{Start: 1, End: 1}.IsInNumberRange(-5))
	assert.False(t, driver_infrastructure.NumberRange{Start: 1, End: 1}.IsInNumberRange(5))

	assert.True(t, driver_infrastructure.NumberRange{Start: 1, End: 5}.IsInNumberRange(1))
	assert.True(t, driver_infrastructure.NumberRange{Start: 1, End: 5}.IsInNumberRange(3))
	assert.True(t, driver_infrastructure.NumberRange{Start: 1, End: 5}.IsInNumberRange(5))
	assert.False(t, driver_infrastructure.NumberRange{Start: 1, End: 5}.IsInNumberRange(-5))
	assert.False(t, driver_infrastructure.NumberRange{Start: 1, End: 5}.IsInNumberRange(0))
	assert.False(t, driver_infrastructure.NumberRange{Start: 1, End: 5}.IsInNumberRange(6))
	assert.False(t, driver_infrastructure.NumberRange{Start: 1, End: 5}.IsInNumberRange(10))
}
