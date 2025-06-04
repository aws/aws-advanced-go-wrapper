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
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetHostWeightMapFromString(t *testing.T) {
	expectedHostWeightMap := map[string]int{
		"host0": 10,
		"host1": 11,
		"host2": 12,
	}
	hostWeightString := "host0:10, host2:12 ,host1:11"
	actualHostWeightMap, err := driver_infrastructure.GetHostWeightMapFromString(hostWeightString)

	assert.Nil(t, err)
	assert.Equal(t, expectedHostWeightMap, actualHostWeightMap)
}

func TestGetHostWeightMapFromStringGivenEmptyString(t *testing.T) {
	expectedHostWeightMap := map[string]int{}
	hostWeightString := ""
	actualHostWeightMap, err := driver_infrastructure.GetHostWeightMapFromString(hostWeightString)

	assert.Nil(t, err)
	assert.Equal(t, expectedHostWeightMap, actualHostWeightMap)
}

func TestGetHostWeightMapFromStringGivenInvalidString(t *testing.T) {
	hostWeightString := "someInvalidString"
	_, err := driver_infrastructure.GetHostWeightMapFromString(hostWeightString)

	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HostSelector.invalidHostWeightPairs"), err.Error())
}

func TestGetHostWeightMapFromStringGivenInvalidHostName(t *testing.T) {
	hostWeightString := "host0:10,:12 ,host1:11"
	_, err := driver_infrastructure.GetHostWeightMapFromString(hostWeightString)

	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HostSelector.invalidHostWeightPairs"), err.Error())
}

func TestGetHostWeightMapFromStringGivenInvalidHostWeight(t *testing.T) {
	hostWeightString := "host0:10,host2:12 ,host1:eleven"
	_, err := driver_infrastructure.GetHostWeightMapFromString(hostWeightString)

	assert.NotNil(t, err)
	assert.Equal(t, error_util.GetMessage("HostSelector.invalidHostWeightPairs"), err.Error())
}
