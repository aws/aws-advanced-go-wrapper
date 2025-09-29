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

	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	"github.com/stretchr/testify/assert"
)

const (
	usEastRegionCluster               = "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionClusterTrailingDot    = "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com."
	usEastRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionInstance              = "instance-test-name.XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionProxy                 = "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.us-east-2.rds.amazonaws.com"

	chinaRegionCluster               = "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionClusterTrailingDot    = "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn."
	chinaRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionInstance              = "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionProxy                 = "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.rds.cn-northwest-1.amazonaws.com.cn"

	oldChinaRegionCluster                          = "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionClusterTrailingDot               = "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn."
	oldChinaRegionClusterReadOnly                  = "database-test-name.cluster-ro-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionInstance                         = "instance-test-name.XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionProxy                            = "proxy-test-name.proxy-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionCustomDomain                     = "custom-test-name.cluster-custom-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionLimitlessDbShardGroup            = "database-test-name.shardgrp-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionLimitlessDbShardGroupTrailingDot = "database-test-name.shardgrp-XYZ.cn-northwest-1.rds.amazonaws.com.cn."

	extraRdsChinaPath      = "database-test-name.cluster-XYZ.rds.cn-northwest-1.rds.amazonaws.com.cn"
	missingCnChinaPath     = "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com"
	missingRegionChinaPath = "database-test-name.cluster-XYZ.rds.amazonaws.com.cn"

	usEastRegionElbUrl            = "elb-name.elb.us-east-2.amazonaws.com"
	usEastRegionElbUrlTrailingDot = "elb-name.elb.us-east-2.amazonaws.com."

	usIsobEastRegionCluster               = "database-test-name.cluster-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionInstance              = "instance-test-name.XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionProxy                 = "proxy-test-name.proxy-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"

	usGovEastRegionCluster               = "database-test-name.cluster-XYZ.rds.us-gov-east-1.amazonaws.com"
	usIsoEastRegionCluster               = "database-test-name.cluster-XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionClusterTrailingDot    = "database-test-name.cluster-XYZ.rds.us-iso-east-1.c2s.ic.gov."
	usIsoEastRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionInstance              = "instance-test-name.XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionProxy                 = "proxy-test-name.proxy-XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.rds.us-iso-east-1.c2s.ic.gov"

	blueInstance  = "myapp-blue.abc123.us-east-1.rds.amazonaws.com"
	greenInstance = "myapp-green-abc123.def456.us-east-1.rds.amazonaws.com"
	oldInstance   = "myapp-old1.ghi789.us-east-1.rds.amazonaws.com"
)

func TestIsRdsDns(t *testing.T) {
	assert.True(t, utils.IsRdsDns(usEastRegionCluster))
	assert.True(t, utils.IsRdsDns(usEastRegionClusterTrailingDot))
	assert.True(t, utils.IsRdsDns(usEastRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(usEastRegionInstance))
	assert.True(t, utils.IsRdsDns(usEastRegionProxy))
	assert.True(t, utils.IsRdsDns(usEastRegionCustomDomain))
	assert.False(t, utils.IsRdsDns(usEastRegionElbUrl))
	assert.False(t, utils.IsRdsDns(usEastRegionElbUrlTrailingDot))
	assert.True(t, utils.IsRdsDns(usEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsRdsDns(usIsobEastRegionCluster))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionInstance))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionProxy))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionCustomDomain))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsRdsDns(usIsoEastRegionCluster))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionClusterTrailingDot))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionInstance))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionProxy))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionCustomDomain))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsRdsDns(chinaRegionCluster))
	assert.True(t, utils.IsRdsDns(chinaRegionClusterTrailingDot))
	assert.True(t, utils.IsRdsDns(chinaRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(chinaRegionInstance))
	assert.True(t, utils.IsRdsDns(chinaRegionProxy))
	assert.True(t, utils.IsRdsDns(chinaRegionCustomDomain))
	assert.True(t, utils.IsRdsDns(chinaRegionLimitlessDbShardGroup))
	assert.True(t, utils.IsRdsDns(extraRdsChinaPath))
	assert.True(t, utils.IsRdsDns(missingCnChinaPath))
	assert.False(t, utils.IsRdsDns(missingRegionChinaPath))

	assert.True(t, utils.IsRdsDns(oldChinaRegionCluster))
	assert.True(t, utils.IsRdsDns(oldChinaRegionClusterTrailingDot))
	assert.True(t, utils.IsRdsDns(oldChinaRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(oldChinaRegionInstance))
	assert.True(t, utils.IsRdsDns(oldChinaRegionProxy))
	assert.True(t, utils.IsRdsDns(oldChinaRegionCustomDomain))
	assert.True(t, utils.IsRdsDns(oldChinaRegionLimitlessDbShardGroup))
	assert.True(t, utils.IsRdsDns(oldChinaRegionLimitlessDbShardGroupTrailingDot))
}

func TestIsWriterClusterDns(t *testing.T) {
	assert.True(t, utils.IsWriterClusterDns(usEastRegionCluster))
	assert.True(t, utils.IsWriterClusterDns(usEastRegionClusterTrailingDot))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionElbUrl))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionElbUrlTrailingDot))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsWriterClusterDns(usIsobEastRegionCluster))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsWriterClusterDns(usIsoEastRegionCluster))
	assert.True(t, utils.IsWriterClusterDns(usIsoEastRegionClusterTrailingDot))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsWriterClusterDns(chinaRegionCluster))
	assert.True(t, utils.IsWriterClusterDns(chinaRegionClusterTrailingDot))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsWriterClusterDns(oldChinaRegionCluster))
	assert.True(t, utils.IsWriterClusterDns(oldChinaRegionClusterTrailingDot))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionLimitlessDbShardGroup))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionLimitlessDbShardGroupTrailingDot))
}

func TestIsReaderClusterDns(t *testing.T) {
	assert.False(t, utils.IsReaderClusterDns(usEastRegionCluster))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionClusterTrailingDot))
	assert.True(t, utils.IsReaderClusterDns(usEastRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionElbUrl))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionElbUrlTrailingDot))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionCluster))
	assert.True(t, utils.IsReaderClusterDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionCluster))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionClusterTrailingDot))
	assert.True(t, utils.IsReaderClusterDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsReaderClusterDns(chinaRegionCluster))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionClusterTrailingDot))
	assert.True(t, utils.IsReaderClusterDns(chinaRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionCluster))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionClusterTrailingDot))
	assert.True(t, utils.IsReaderClusterDns(oldChinaRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionLimitlessDbShardGroup))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionLimitlessDbShardGroupTrailingDot))
}

func TestIsLimitlessDbShardGroupDns(t *testing.T) {
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionClusterTrailingDot))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionCustomDomain))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionElbUrl))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionElbUrlTrailingDot))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(usEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionCustomDomain))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionClusterTrailingDot))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionCustomDomain))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionClusterTrailingDot))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionCustomDomain))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(chinaRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionClusterTrailingDot))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionCustomDomain))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionLimitlessDbShardGroup))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionLimitlessDbShardGroupTrailingDot))
}

func TestGetRdsRegion(t *testing.T) {
	assert.Equal(t, "us-east-2", utils.GetRdsRegion(usEastRegionCluster))
	assert.Equal(t, "cn-northwest-1", utils.GetRdsRegion(chinaRegionClusterReadOnly))
	assert.Equal(t, "us-isob-east-1", utils.GetRdsRegion(usIsobEastRegionCluster))
	assert.Equal(t, "us-iso-east-1", utils.GetRdsRegion(usIsoEastRegionCluster))
}

func TestGetRdsClusterHostUrl(t *testing.T) {
	assert.Equal(t, "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", utils.GetRdsClusterHostUrl(usEastRegionCluster))
	assert.Equal(t, "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", utils.GetRdsClusterHostUrl(usEastRegionClusterReadOnly))
	assert.Equal(t, "instance-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", utils.GetRdsClusterHostUrl(usEastRegionInstance))
	assert.Equal(t, "proxy-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", utils.GetRdsClusterHostUrl(usEastRegionProxy))
	assert.Equal(t, "custom-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", utils.GetRdsClusterHostUrl(usEastRegionCustomDomain))
	assert.Equal(t, "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", utils.GetRdsClusterHostUrl(usEastRegionLimitlessDbShardGroup))

	assert.Equal(t, "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn", utils.GetRdsClusterHostUrl(chinaRegionClusterReadOnly))
	assert.Equal(t, "database-test-name.cluster-XYZ.rds.us-isob-east-1.sc2s.sgov.gov", utils.GetRdsClusterHostUrl(usIsobEastRegionCluster))
	assert.Equal(t, "database-test-name.cluster-XYZ.rds.us-iso-east-1.c2s.ic.gov", utils.GetRdsClusterHostUrl(usIsoEastRegionCluster))
}

func TestIsRdsInstance(t *testing.T) {
	assert.True(t, utils.IsRdsInstance(usEastRegionInstance), "Should identify RDS instance")
	assert.True(t, utils.IsRdsInstance(chinaRegionInstance), "Should identify China RDS instance")
	assert.True(t, utils.IsRdsInstance(oldChinaRegionInstance), "Should identify old China RDS instance")

	assert.False(t, utils.IsRdsInstance(usEastRegionCluster), "Should not identify cluster as instance")
	assert.False(t, utils.IsRdsInstance(usEastRegionClusterReadOnly), "Should not identify read-only cluster as instance")
	assert.False(t, utils.IsRdsInstance(chinaRegionCluster), "Should not identify China cluster as instance")

	assert.False(t, utils.IsRdsInstance(usEastRegionProxy), "Should not identify proxy as instance")
	assert.False(t, utils.IsRdsInstance(chinaRegionProxy), "Should not identify China proxy as instance")

	assert.False(t, utils.IsRdsInstance("example.com"), "Should not identify non-RDS host as instance")
	assert.False(t, utils.IsRdsInstance("database.example.org"), "Should not identify non-RDS host as instance")
	assert.False(t, utils.IsRdsInstance(""), "Should not identify empty string as instance")

	assert.True(t, utils.IsRdsInstance("instance-test-name.XYZ.us-east-2.rds.amazonaws.com."), "Should handle trailing dot")

	assert.True(t, utils.IsRdsInstance(blueInstance), "Should identify blue instance as RDS instance")
	assert.True(t, utils.IsRdsInstance(greenInstance), "Should identify green instance as RDS instance")
	assert.True(t, utils.IsRdsInstance(oldInstance), "Should identify old instance as RDS instance")
}

func TestIsGreenInstance(t *testing.T) {
	greenInstances := []string{
		"myapp-green-abc123.def456.us-east-1.rds.amazonaws.com",
		"database-green-xyz789.cluster-abc123.us-west-2.rds.amazonaws.com",
		"test-green-123abc.instance.eu-west-1.rds.amazonaws.com",
		"prod-GREEN-456DEF.cluster.ap-southeast-1.rds.amazonaws.com", // case-insensitive
	}

	for _, host := range greenInstances {
		assert.True(t, utils.IsGreenInstance(host), "Should identify green instance: %s", host)
	}

	nonGreenInstances := []string{
		blueInstance,
		oldInstance,
		"test-instance.xyz789.eu-west-1.rds.amazonaws.com",
		"prod-cluster.cluster-abc123.ap-southeast-1.rds.amazonaws.com",
		"example.com",
		"",
		"myapp-greenish.abc123.us-east-1.rds.amazonaws.com",
		"myapp-green.abc123.us-east-1.rds.amazonaws.com",
	}
	for _, host := range nonGreenInstances {
		assert.False(t, utils.IsGreenInstance(host), "Should not identify as green instance: %s", host)
	}

	assert.False(t, utils.IsGreenInstance(""), "Should return false for empty string")
	assert.False(t, utils.IsGreenInstance("   "), "Should return false for whitespace")
}

func TestIsNotOldInstance(t *testing.T) {
	nonOldInstances := []string{
		"myapp-old1ish.abc123.us-east-1.rds.amazonaws.com",
		"prod-old1-cluster.cluster-abc123.ap-southeast-1.rds.amazonaws.com",
		"myapp-blue.abc123.us-east-1.rds.amazonaws.com",
		"database-green-xyz789.def456.us-west-2.rds.amazonaws.com",
		"test-instance.ghi789.eu-west-1.rds.amazonaws.com",
		"prod-cluster.cluster-abc123.ap-southeast-1.rds.amazonaws.com",
		"example.com",
		"",
		"   ",
	}

	for _, host := range nonOldInstances {
		assert.True(t, utils.IsNotOldInstance(host), "Should identify as not old instance: %s", host)
	}

	oldInstances := []string{
		"myapp-old1.abc123.us-east-1.rds.amazonaws.com",
		"database-old1.def456.us-west-2.rds.amazonaws.com",
		"test-OLD1.ghi789.eu-west-1.rds.amazonaws.com",
	}

	for _, host := range oldInstances {
		assert.False(t, utils.IsNotOldInstance(host), "Should identify as old instance: %s", host)
	}
}

func TestIsNotGreenAndNotOldInstance(t *testing.T) {
	blueInstances := []string{
		blueInstance,
		"database-prod.def456.us-west-2.rds.amazonaws.com",
		"test-instance.ghi789.eu-west-1.rds.amazonaws.com",
		"prod-cluster.cluster-abc123.ap-southeast-1.rds.amazonaws.com",
	}

	for _, host := range blueInstances {
		assert.True(t, utils.IsNotGreenAndNotOldInstance(host), "Should identify not green and not old: %s", host)
	}

	greenInstances := []string{
		greenInstance,
		"database-green-xyz789.cluster-abc123.us-west-2.rds.amazonaws.com",
	}

	for _, host := range greenInstances {
		assert.False(t, utils.IsNotGreenAndNotOldInstance(host), "Should identify as green instance: %s", host)
	}

	oldInstances := []string{
		oldInstance,
		"database-old1.def456.us-west-2.rds.amazonaws.com",
	}

	for _, host := range oldInstances {
		assert.False(t, utils.IsNotGreenAndNotOldInstance(host), "Should identify as old instance: %s", host)
	}

	assert.False(t, utils.IsNotGreenAndNotOldInstance(""), "Should return false for empty string")
}

func TestRemoveGreenInstancePrefix(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
		desc     string
	}{
		{
			input:    "myapp-green-abc123.def456.us-east-1.rds.amazonaws.com",
			expected: "myapp.def456.us-east-1.rds.amazonaws.com",
			desc:     "Should remove green prefix from standard green instance",
		},
		{
			input:    "database-green-xyz789.cluster-abc123.us-west-2.rds.amazonaws.com",
			expected: "database.cluster-abc123.us-west-2.rds.amazonaws.com",
			desc:     "Should remove green prefix from green cluster",
		},
		{
			input:    "test-GREEN-123ABC.instance.eu-west-1.rds.amazonaws.com",
			expected: "test.instance.eu-west-1.rds.amazonaws.com",
			desc:     "Should handle case insensitive green prefix",
		},
	}

	for _, tc := range testCases {
		result := utils.RemoveGreenInstancePrefix(tc.input)
		assert.Equal(t, tc.expected, result, tc.desc)
	}

	nonGreenInstances := []string{
		"myapp-blue.abc123.us-east-1.rds.amazonaws.com",
		"database-old1.def456.us-west-2.rds.amazonaws.com",
		"test-instance.ghi789.eu-west-1.rds.amazonaws.com",
		"example.com",
		"",
	}

	for _, host := range nonGreenInstances {
		result := utils.RemoveGreenInstancePrefix(host)
		assert.Equal(t, host, result, "Should return unchanged for non-green instance: %s", host)
	}

	assert.Equal(t, "", utils.RemoveGreenInstancePrefix(""), "Should handle empty string")

	// Test hostid pattern fallback
	hostIdPattern := "myapp-green-abc123"
	expectedHostId := "myapp"
	result := utils.RemoveGreenInstancePrefix(hostIdPattern)
	assert.Equal(t, expectedHostId, result)
}

func TestGetRdsClusterId(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
		desc     string
	}{
		{
			input:    usEastRegionCluster,
			expected: "database-test-name",
			desc:     "Should extract cluster ID from US East cluster",
		},
		{
			input:    usEastRegionClusterReadOnly,
			expected: "database-test-name",
			desc:     "Should extract cluster ID from read-only cluster",
		},
		{
			input:    chinaRegionCluster,
			expected: "database-test-name",
			desc:     "Should extract cluster ID from China cluster",
		},
		{
			input:    oldChinaRegionCluster,
			expected: "database-test-name",
			desc:     "Should extract cluster ID from old China cluster",
		},
		{
			input:    usEastRegionClusterTrailingDot,
			expected: "database-test-name",
			desc:     "Should handle trailing dot",
		},
		{
			input:    usEastRegionCustomDomain,
			expected: "custom-test-name",
			desc:     "Should extract cluster ID from custom domain",
		},
		{
			input:    usEastRegionInstance,
			expected: "instance-test-name",
			desc:     "Should extract instance ID from US East instance",
		},
		{
			input:    chinaRegionInstance,
			expected: "instance-test-name",
			desc:     "Should extract instance ID from China instance",
		},
		{
			input:    "myapp-blue.cluster-abc123.us-east-1.rds.amazonaws.com",
			expected: "myapp-blue",
			desc:     "Should extract ID from blue cluster",
		},
		{
			input:    "myapp-green-def456.cluster-xyz789.us-west-2.rds.amazonaws.com",
			expected: "myapp-green-def456",
			desc:     "Should extract ID from green cluster",
		},
		{
			input:    "myapp-old1.cluster-ghi789.eu-west-1.rds.amazonaws.com",
			expected: "myapp-old1",
			desc:     "Should extract ID from old cluster",
		},
	}

	for _, tc := range testCases {
		result := utils.GetRdsClusterId(tc.input)
		assert.Equal(t, tc.expected, result, tc.desc)
	}

	nonRdsHosts := []string{
		"example.com",
		"database.example.org",
		"localhost",
		"192.168.1.1",
		"   ",
		"",
	}

	for _, host := range nonRdsHosts {
		result := utils.GetRdsClusterId(host)
		assert.Equal(t, "", result, "Should return empty string for non-RDS host: %s", host)
	}
}
