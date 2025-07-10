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

	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"

	"github.com/stretchr/testify/assert"
)

func TestGetRegionFromRegionString(t *testing.T) {
	assert.Equal(t, region_util.Region(""), region_util.GetRegionFromRegionString("someRandomUnknownRegion"))
	assert.Equal(t, region_util.AP_SOUTH_1, region_util.GetRegionFromRegionString(string(region_util.AP_SOUTH_1)))
	assert.Equal(t, region_util.AP_SOUTH_1, region_util.GetRegionFromRegionString("ap-south-1"))
	assert.Equal(t, region_util.AP_SOUTH_1, region_util.GetRegionFromRegionString("Ap-SoUtH-1"))
	assert.Equal(t, "", "")
}

func TestGetRegionFromHost(t *testing.T) {
	assert.Equal(t, region_util.US_EAST_2, region_util.GetRegionFromHost(usEastRegionCluster))
	assert.Equal(t, region_util.CN_NORTHWEST_1, region_util.GetRegionFromHost(chinaRegionCluster))
	assert.Equal(t, region_util.CN_NORTHWEST_1, region_util.GetRegionFromHost(oldChinaRegionCluster))
	assert.Equal(t, region_util.US_ISOB_EAST_1, region_util.GetRegionFromHost(usIsobEastRegionCluster))
	assert.Equal(t, region_util.US_GOV_EAST_1, region_util.GetRegionFromHost(usGovEastRegionCluster))
}

func TestGetRegionFromProps(t *testing.T) {
	props := map[string]string{
		property_util.IAM_REGION.Name: "me-south-1",
	}
	assert.Equal(t, region_util.ME_SOUTH_1, region_util.GetRegionFromProps(props, property_util.IAM_REGION))
}

func TestGetRegion(t *testing.T) {
	propsWithIamRegion := map[string]string{
		property_util.IAM_REGION.Name: "me-south-1",
	}
	propsWithoutIamRegion := map[string]string{}

	assert.Equal(t, region_util.ME_SOUTH_1, region_util.GetRegion("", propsWithIamRegion, property_util.IAM_REGION))
	assert.Equal(t, region_util.US_GOV_EAST_1, region_util.GetRegion(usGovEastRegionCluster, propsWithoutIamRegion, property_util.IAM_REGION))
	assert.Equal(t, region_util.Region(""), region_util.GetRegion("", propsWithoutIamRegion, property_util.IAM_REGION))
}
