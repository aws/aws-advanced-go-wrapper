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
	"awssql/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	usEastRegionCluster               = "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionInstance              = "instance-test-name.XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionProxy                 = "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com"
	usEastRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.us-east-2.rds.amazonaws.com"

	chinaRegionCluster               = "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionInstance              = "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionProxy                 = "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
	chinaRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.rds.cn-northwest-1.amazonaws.com.cn"

	oldChinaRegionCluster               = "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionInstance              = "instance-test-name.XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionProxy                 = "proxy-test-name.proxy-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
	oldChinaRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.cn-northwest-1.rds.amazonaws.com.cn"

	extraRdsChinaPath      = "database-test-name.cluster-XYZ.rds.cn-northwest-1.rds.amazonaws.com.cn" //nolint:unused
	missingCnChinaPath     = "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com"        //nolint:unused
	missingRegionChinaPath = "database-test-name.cluster-XYZ.rds.amazonaws.com.cn"                    //nolint:unused

	usEastRegionElbUrl = "elb-name.elb.us-east-2.amazonaws.com"

	usIsobEastRegionCluster               = "database-test-name.cluster-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionInstance              = "instance-test-name.XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionProxy                 = "proxy-test-name.proxy-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
	usIsobEastRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"

	usGovEastRegionCluster               = "database-test-name.cluster-XYZ.rds.us-gov-east-1.amazonaws.com"
	usIsoEastRegionCluster               = "database-test-name.cluster-XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionClusterReadOnly       = "database-test-name.cluster-ro-XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionInstance              = "instance-test-name.XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionProxy                 = "proxy-test-name.proxy-XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionCustomDomain          = "custom-test-name.cluster-custom-XYZ.rds.us-iso-east-1.c2s.ic.gov"
	usIsoEastRegionLimitlessDbShardGroup = "database-test-name.shardgrp-XYZ.rds.us-iso-east-1.c2s.ic.gov"
)

func TestIsRdsDns(t *testing.T) {
	assert.True(t, utils.IsRdsDns(usEastRegionCluster))
	assert.True(t, utils.IsRdsDns(usEastRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(usEastRegionInstance))
	assert.True(t, utils.IsRdsDns(usEastRegionProxy))
	assert.True(t, utils.IsRdsDns(usEastRegionCustomDomain))
	assert.False(t, utils.IsRdsDns(usEastRegionElbUrl))
	assert.True(t, utils.IsRdsDns(usEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsRdsDns(usIsobEastRegionCluster))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionInstance))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionProxy))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionCustomDomain))
	assert.True(t, utils.IsRdsDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsRdsDns(usIsoEastRegionCluster))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionInstance))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionProxy))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionCustomDomain))
	assert.True(t, utils.IsRdsDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsRdsDns(chinaRegionCluster))
	assert.True(t, utils.IsRdsDns(chinaRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(chinaRegionInstance))
	assert.True(t, utils.IsRdsDns(chinaRegionProxy))
	assert.True(t, utils.IsRdsDns(chinaRegionCustomDomain))
	assert.True(t, utils.IsRdsDns(chinaRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsRdsDns(oldChinaRegionCluster))
	assert.True(t, utils.IsRdsDns(oldChinaRegionClusterReadOnly))
	assert.True(t, utils.IsRdsDns(oldChinaRegionInstance))
	assert.True(t, utils.IsRdsDns(oldChinaRegionProxy))
	assert.True(t, utils.IsRdsDns(oldChinaRegionCustomDomain))
	assert.True(t, utils.IsRdsDns(oldChinaRegionLimitlessDbShardGroup))
}

func TestIsWriterClusterDns(t *testing.T) {
	assert.True(t, utils.IsWriterClusterDns(usEastRegionCluster))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionElbUrl))
	assert.False(t, utils.IsWriterClusterDns(usEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsWriterClusterDns(usIsobEastRegionCluster))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsWriterClusterDns(usIsoEastRegionCluster))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsWriterClusterDns(chinaRegionCluster))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(chinaRegionLimitlessDbShardGroup))

	assert.True(t, utils.IsWriterClusterDns(oldChinaRegionCluster))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionClusterReadOnly))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionInstance))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionProxy))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionCustomDomain))
	assert.False(t, utils.IsWriterClusterDns(oldChinaRegionLimitlessDbShardGroup))
}

func TestIsReaderClusterDns(t *testing.T) {
	assert.False(t, utils.IsReaderClusterDns(usEastRegionCluster))
	assert.True(t, utils.IsReaderClusterDns(usEastRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionElbUrl))
	assert.False(t, utils.IsReaderClusterDns(usEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionCluster))
	assert.True(t, utils.IsReaderClusterDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionCluster))
	assert.True(t, utils.IsReaderClusterDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsReaderClusterDns(chinaRegionCluster))
	assert.True(t, utils.IsReaderClusterDns(chinaRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(chinaRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionCluster))
	assert.True(t, utils.IsReaderClusterDns(oldChinaRegionClusterReadOnly))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionInstance))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionProxy))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionCustomDomain))
	assert.False(t, utils.IsReaderClusterDns(oldChinaRegionLimitlessDbShardGroup))
}

func TestIsLimitlessDbShardGroupDns(t *testing.T) {
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionCustomDomain))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usEastRegionElbUrl))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(usEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionCustomDomain))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionCustomDomain))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(chinaRegionCustomDomain))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(chinaRegionLimitlessDbShardGroup))

	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionCluster))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionClusterReadOnly))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionInstance))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionProxy))
	assert.False(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionCustomDomain))
	assert.True(t, utils.IsLimitlessDbShardGroupDns(oldChinaRegionLimitlessDbShardGroup))
}
