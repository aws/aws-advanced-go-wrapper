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
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/stretchr/testify/assert"
)

const TEST_PORT = 1234

func TestRoundRobinHostSelector(t *testing.T) {
	// Test hosts
	host0 := "host-0"
	host1 := "host-1"
	host2 := "host-2"
	host3 := "host-3"
	host4 := "host-4"
	unavailableHost := "unavailable-host"

	instance0 := "instance-0"
	instance1 := "instance-1"
	instance2 := "instance-2"
	instance3 := "instance-3"
	instance4 := "instance-4"
	unavailableInstance := "unavailable-instance"

	writerHost := &host_info_util.HostInfo{
		Host:         host0,
		HostId:       instance0,
		Port:         TEST_PORT,
		Role:         host_info_util.WRITER,
		Availability: host_info_util.AVAILABLE,
	}
	readerHost1 := &host_info_util.HostInfo{
		Host:         host1,
		HostId:       instance1,
		Port:         TEST_PORT,
		Role:         host_info_util.READER,
		Availability: host_info_util.AVAILABLE,
	}
	readerHost2 := &host_info_util.HostInfo{
		Host:         host2,
		HostId:       instance2,
		Port:         TEST_PORT,
		Role:         host_info_util.READER,
		Availability: host_info_util.AVAILABLE,
	}
	readerHost2Unavailable := &host_info_util.HostInfo{
		Host:         host2,
		HostId:       instance2,
		Port:         TEST_PORT,
		Role:         host_info_util.READER,
		Availability: host_info_util.UNAVAILABLE,
	}
	readerHost3 := &host_info_util.HostInfo{
		Host:         host3,
		HostId:       instance3,
		Port:         TEST_PORT,
		Role:         host_info_util.READER,
		Availability: host_info_util.AVAILABLE,
	}
	readerHost4 := &host_info_util.HostInfo{
		Host:         host4,
		HostId:       instance4,
		Port:         TEST_PORT,
		Role:         host_info_util.READER,
		Availability: host_info_util.AVAILABLE,
	}
	unavailableReaderHost := &host_info_util.HostInfo{
		Host:         unavailableHost,
		HostId:       unavailableInstance,
		Port:         TEST_PORT,
		Role:         host_info_util.READER,
		Availability: host_info_util.UNAVAILABLE,
	}

	hostsList123 := []*host_info_util.HostInfo{writerHost, readerHost2, readerHost3, readerHost1}
	hostsList12Unavailable3 := []*host_info_util.HostInfo{writerHost, readerHost2Unavailable, readerHost3, readerHost1}

	hostsList1234 := []*host_info_util.HostInfo{writerHost, readerHost4, readerHost2, readerHost3, readerHost1}
	hostsList13 := []*host_info_util.HostInfo{writerHost, readerHost3, readerHost1}
	hostsList14 := []*host_info_util.HostInfo{writerHost, readerHost4, readerHost1}
	hostsList23 := []*host_info_util.HostInfo{writerHost, readerHost3, readerHost2}
	hostListUnavailable := []*host_info_util.HostInfo{writerHost, unavailableReaderHost}
	hostList1Unavailable := []*host_info_util.HostInfo{writerHost, readerHost1, unavailableReaderHost}

	writerHostsList := []*host_info_util.HostInfo{writerHost}

	t.Run("test setup with incorrect host weight pairs", func(t *testing.T) {
		testCases := []string{
			instance0 + ":1,3," + instance2 + ":2," + instance3 + ":3",
			instance0 + ":1," + instance1 + "," + instance2 + ":2," + instance3 + ":3",
			instance0 + ":1," + instance1 + ":0," + instance2 + ":2," + instance3 + ":3",
			instance0 + ":1," + instance1 + ":1:3," + instance2 + ":2," + instance3 + ":3",
			instance0 + ":1," + instance1 + ":1.123," + instance2 + ":2.456," + instance3 + ":3.789",
			instance0 + ":1," + instance1 + ":-1," + instance2 + ":-2," + instance3 + ":-3",
			instance0 + ":1," + instance1 + ":1a," + instance2 + ":b",
		}

		for _, hostWeights := range testCases {
			selector := driver_infrastructure.GetRoundRobinHostSelector()
			props := map[string]string{
				property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS.Name: hostWeights,
			}
			_, err := selector.GetHost(hostsList123, host_info_util.READER, props)
			assert.Error(t, err)
			assert.Equal(t, error_util.GetMessage("HostSelector.invalidHostWeightPairs"), err.Error())
			selector.ClearCache()
		}
	})

	t.Run("test setup with incorrect default weight", func(t *testing.T) {
		testCases := []string{"0", "1.123", "-1"}

		for _, defaultWeight := range testCases {
			selector := driver_infrastructure.GetRoundRobinHostSelector()
			props := map[string]string{
				property_util.ROUND_ROBIN_DEFAULT_WEIGHT.Name: defaultWeight,
			}
			_, err := selector.GetHost(hostsList123, host_info_util.READER, props)
			assert.Error(t, err)
			assert.Equal(t, error_util.GetMessage("HostSelector.roundRobinInvalidDefaultWeight"), err.Error())
			selector.ClearCache()
		}
	})

	t.Run("test getHost no readers", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}
		_, err := selector.GetHost(writerHostsList, host_info_util.READER, props)
		assert.Error(t, err)
		assert.Equal(t, error_util.GetMessage("HostSelector.noHostsMatchingRole", host_info_util.READER), err.Error())
		selector.ClearCache()
	})

	t.Run("test getHost", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		host, err := selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost AvailabilityChanged", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		host, err := selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		// availability changed in host list, should continue to host 3
		host, err = selector.GetHost(hostsList12Unavailable3, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)
		selector.ClearCache()
	})

	t.Run("test getHost undefined properties", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()

		host, err := selector.GetHost(hostsList123, host_info_util.READER, nil)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, nil)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, nil)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, nil)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost defaultWeightChanged properties", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()

		props := map[string]string{
			property_util.ROUND_ROBIN_DEFAULT_WEIGHT.Name: "1",
		}

		host, err := selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		// Change property to 2
		property_util.ROUND_ROBIN_DEFAULT_WEIGHT.Set(props, "2")

		host, err = selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		host, err = selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost weighted", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		hostWeights := instance0 + ":1," + instance1 + ":3," + instance2 + ":2," + instance3 + ":1"
		props := map[string]string{
			property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS.Name: hostWeights,
		}

		host, err := selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost weighted change properties", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		hostWeights := instance0 + ":1," + instance1 + ":2," + instance2 + ":2," + instance3 + ":1"
		hostWeights2 := instance0 + ":1," + instance1 + ":2," + instance2 + ":1," + instance3 + ":1"
		hostWeights3 := instance0 + ":1," + instance1 + ":1," + instance2 + ":1," + instance3 + ":1"

		props := map[string]string{
			property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS.Name: hostWeights,
		}

		host, err := selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS.Set(props, hostWeights2)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		// Start the round robin again but change after it gets readerHost2 and make sure it goes back to host 1 and resets.

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		property_util.ROUND_ROBIN_HOST_WEIGHT_PAIRS.Set(props, hostWeights3)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost cache entry expired", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		host, err := selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		selector.ClearCache()

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost scale up", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		host, err := selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		host, err = selector.GetHost(hostsList1234, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost4.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost scale down", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		host, err := selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost last host not in hosts list", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		host, err := selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList123, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList13, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost3.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost all hosts changed", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		host, err := selector.GetHost(hostsList14, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		host, err = selector.GetHost(hostsList23, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost2.Host, host.Host)

		host, err = selector.GetHost(hostsList14, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost4.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost unavailableHost", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		_, err := selector.GetHost(hostListUnavailable, host_info_util.READER, props)
		assert.Error(t, err)
		assert.Equal(t, error_util.GetMessage("HostSelector.noHostsMatchingRole", host_info_util.READER), err.Error())

		host, err := selector.GetHost(hostList1Unavailable, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost noAvailableHosts", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		_, err := selector.GetHost(hostListUnavailable, host_info_util.READER, props)
		assert.Error(t, err)
		assert.Equal(t, error_util.GetMessage("HostSelector.noHostsMatchingRole", host_info_util.READER), err.Error())

		host, err := selector.GetHost(hostList1Unavailable, host_info_util.READER, props)
		assert.NoError(t, err)
		assert.Equal(t, readerHost1.Host, host.Host)

		selector.ClearCache()
	})

	t.Run("test getHost emptyHostList", func(t *testing.T) {
		selector := driver_infrastructure.GetRoundRobinHostSelector()
		props := map[string]string{}

		_, err := selector.GetHost([]*host_info_util.HostInfo{}, host_info_util.READER, props)
		assert.Error(t, err)
		assert.Equal(t, error_util.GetMessage("HostSelector.noHostsMatchingRole", host_info_util.READER), err.Error())
	})
}
