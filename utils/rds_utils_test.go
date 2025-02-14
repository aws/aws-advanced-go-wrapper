package utils

import (
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
	assert.True(t, IsRdsDns(usEastRegionCluster))
	assert.True(t, IsRdsDns(usEastRegionClusterReadOnly))
	assert.True(t, IsRdsDns(usEastRegionInstance))
	assert.True(t, IsRdsDns(usEastRegionProxy))
	assert.True(t, IsRdsDns(usEastRegionCustomDomain))
	assert.False(t, IsRdsDns(usEastRegionElbUrl))
	assert.True(t, IsRdsDns(usEastRegionLimitlessDbShardGroup))

	assert.True(t, IsRdsDns(usIsobEastRegionCluster))
	assert.True(t, IsRdsDns(usIsobEastRegionClusterReadOnly))
	assert.True(t, IsRdsDns(usIsobEastRegionInstance))
	assert.True(t, IsRdsDns(usIsobEastRegionProxy))
	assert.True(t, IsRdsDns(usIsobEastRegionCustomDomain))
	assert.True(t, IsRdsDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.True(t, IsRdsDns(usIsoEastRegionCluster))
	assert.True(t, IsRdsDns(usIsoEastRegionClusterReadOnly))
	assert.True(t, IsRdsDns(usIsoEastRegionInstance))
	assert.True(t, IsRdsDns(usIsoEastRegionProxy))
	assert.True(t, IsRdsDns(usIsoEastRegionCustomDomain))
	assert.True(t, IsRdsDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.True(t, IsRdsDns(chinaRegionCluster))
	assert.True(t, IsRdsDns(chinaRegionClusterReadOnly))
	assert.True(t, IsRdsDns(chinaRegionInstance))
	assert.True(t, IsRdsDns(chinaRegionProxy))
	assert.True(t, IsRdsDns(chinaRegionCustomDomain))
	assert.True(t, IsRdsDns(chinaRegionLimitlessDbShardGroup))

	assert.True(t, IsRdsDns(oldChinaRegionCluster))
	assert.True(t, IsRdsDns(oldChinaRegionClusterReadOnly))
	assert.True(t, IsRdsDns(oldChinaRegionInstance))
	assert.True(t, IsRdsDns(oldChinaRegionProxy))
	assert.True(t, IsRdsDns(oldChinaRegionCustomDomain))
	assert.True(t, IsRdsDns(oldChinaRegionLimitlessDbShardGroup))
}

func TestIsWriterClusterDns(t *testing.T) {
	assert.True(t, IsWriterClusterDns(usEastRegionCluster))
	assert.False(t, IsWriterClusterDns(usEastRegionClusterReadOnly))
	assert.False(t, IsWriterClusterDns(usEastRegionInstance))
	assert.False(t, IsWriterClusterDns(usEastRegionProxy))
	assert.False(t, IsWriterClusterDns(usEastRegionCustomDomain))
	assert.False(t, IsWriterClusterDns(usEastRegionElbUrl))
	assert.False(t, IsWriterClusterDns(usEastRegionLimitlessDbShardGroup))

	assert.True(t, IsWriterClusterDns(usIsobEastRegionCluster))
	assert.False(t, IsWriterClusterDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, IsWriterClusterDns(usIsobEastRegionInstance))
	assert.False(t, IsWriterClusterDns(usIsobEastRegionProxy))
	assert.False(t, IsWriterClusterDns(usIsobEastRegionCustomDomain))
	assert.False(t, IsWriterClusterDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.True(t, IsWriterClusterDns(usIsoEastRegionCluster))
	assert.False(t, IsWriterClusterDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, IsWriterClusterDns(usIsoEastRegionInstance))
	assert.False(t, IsWriterClusterDns(usIsoEastRegionProxy))
	assert.False(t, IsWriterClusterDns(usIsoEastRegionCustomDomain))
	assert.False(t, IsWriterClusterDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.True(t, IsWriterClusterDns(chinaRegionCluster))
	assert.False(t, IsWriterClusterDns(chinaRegionClusterReadOnly))
	assert.False(t, IsWriterClusterDns(chinaRegionInstance))
	assert.False(t, IsWriterClusterDns(chinaRegionProxy))
	assert.False(t, IsWriterClusterDns(chinaRegionCustomDomain))
	assert.False(t, IsWriterClusterDns(chinaRegionLimitlessDbShardGroup))

	assert.True(t, IsWriterClusterDns(oldChinaRegionCluster))
	assert.False(t, IsWriterClusterDns(oldChinaRegionClusterReadOnly))
	assert.False(t, IsWriterClusterDns(oldChinaRegionInstance))
	assert.False(t, IsWriterClusterDns(oldChinaRegionProxy))
	assert.False(t, IsWriterClusterDns(oldChinaRegionCustomDomain))
	assert.False(t, IsWriterClusterDns(oldChinaRegionLimitlessDbShardGroup))
}

func TestIsReaderClusterDns(t *testing.T) {
	assert.False(t, IsReaderClusterDns(usEastRegionCluster))
	assert.True(t, IsReaderClusterDns(usEastRegionClusterReadOnly))
	assert.False(t, IsReaderClusterDns(usEastRegionInstance))
	assert.False(t, IsReaderClusterDns(usEastRegionProxy))
	assert.False(t, IsReaderClusterDns(usEastRegionCustomDomain))
	assert.False(t, IsReaderClusterDns(usEastRegionElbUrl))
	assert.False(t, IsReaderClusterDns(usEastRegionLimitlessDbShardGroup))

	assert.False(t, IsReaderClusterDns(usIsobEastRegionCluster))
	assert.True(t, IsReaderClusterDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, IsReaderClusterDns(usIsobEastRegionInstance))
	assert.False(t, IsReaderClusterDns(usIsobEastRegionProxy))
	assert.False(t, IsReaderClusterDns(usIsobEastRegionCustomDomain))
	assert.False(t, IsReaderClusterDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.False(t, IsReaderClusterDns(usIsoEastRegionCluster))
	assert.True(t, IsReaderClusterDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, IsReaderClusterDns(usIsoEastRegionInstance))
	assert.False(t, IsReaderClusterDns(usIsoEastRegionProxy))
	assert.False(t, IsReaderClusterDns(usIsoEastRegionCustomDomain))
	assert.False(t, IsReaderClusterDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.False(t, IsReaderClusterDns(chinaRegionCluster))
	assert.True(t, IsReaderClusterDns(chinaRegionClusterReadOnly))
	assert.False(t, IsReaderClusterDns(chinaRegionInstance))
	assert.False(t, IsReaderClusterDns(chinaRegionProxy))
	assert.False(t, IsReaderClusterDns(chinaRegionCustomDomain))
	assert.False(t, IsReaderClusterDns(chinaRegionLimitlessDbShardGroup))

	assert.False(t, IsReaderClusterDns(oldChinaRegionCluster))
	assert.True(t, IsReaderClusterDns(oldChinaRegionClusterReadOnly))
	assert.False(t, IsReaderClusterDns(oldChinaRegionInstance))
	assert.False(t, IsReaderClusterDns(oldChinaRegionProxy))
	assert.False(t, IsReaderClusterDns(oldChinaRegionCustomDomain))
	assert.False(t, IsReaderClusterDns(oldChinaRegionLimitlessDbShardGroup))
}

func TestIsLimitlessDbShardGroupDns(t *testing.T) {
	assert.False(t, IsLimitlessDbShardGroupDns(usEastRegionCluster))
	assert.False(t, IsLimitlessDbShardGroupDns(usEastRegionClusterReadOnly))
	assert.False(t, IsLimitlessDbShardGroupDns(usEastRegionInstance))
	assert.False(t, IsLimitlessDbShardGroupDns(usEastRegionProxy))
	assert.False(t, IsLimitlessDbShardGroupDns(usEastRegionCustomDomain))
	assert.False(t, IsLimitlessDbShardGroupDns(usEastRegionElbUrl))
	assert.True(t, IsLimitlessDbShardGroupDns(usEastRegionLimitlessDbShardGroup))

	assert.False(t, IsLimitlessDbShardGroupDns(usIsobEastRegionCluster))
	assert.False(t, IsLimitlessDbShardGroupDns(usIsobEastRegionClusterReadOnly))
	assert.False(t, IsLimitlessDbShardGroupDns(usIsobEastRegionInstance))
	assert.False(t, IsLimitlessDbShardGroupDns(usIsobEastRegionProxy))
	assert.False(t, IsLimitlessDbShardGroupDns(usIsobEastRegionCustomDomain))
	assert.True(t, IsLimitlessDbShardGroupDns(usIsobEastRegionLimitlessDbShardGroup))

	assert.False(t, IsLimitlessDbShardGroupDns(usIsoEastRegionCluster))
	assert.False(t, IsLimitlessDbShardGroupDns(usIsoEastRegionClusterReadOnly))
	assert.False(t, IsLimitlessDbShardGroupDns(usIsoEastRegionInstance))
	assert.False(t, IsLimitlessDbShardGroupDns(usIsoEastRegionProxy))
	assert.False(t, IsLimitlessDbShardGroupDns(usIsoEastRegionCustomDomain))
	assert.True(t, IsLimitlessDbShardGroupDns(usIsoEastRegionLimitlessDbShardGroup))

	assert.False(t, IsLimitlessDbShardGroupDns(chinaRegionCluster))
	assert.False(t, IsLimitlessDbShardGroupDns(chinaRegionClusterReadOnly))
	assert.False(t, IsLimitlessDbShardGroupDns(chinaRegionInstance))
	assert.False(t, IsLimitlessDbShardGroupDns(chinaRegionProxy))
	assert.False(t, IsLimitlessDbShardGroupDns(chinaRegionCustomDomain))
	assert.True(t, IsLimitlessDbShardGroupDns(chinaRegionLimitlessDbShardGroup))

	assert.False(t, IsLimitlessDbShardGroupDns(oldChinaRegionCluster))
	assert.False(t, IsLimitlessDbShardGroupDns(oldChinaRegionClusterReadOnly))
	assert.False(t, IsLimitlessDbShardGroupDns(oldChinaRegionInstance))
	assert.False(t, IsLimitlessDbShardGroupDns(oldChinaRegionProxy))
	assert.False(t, IsLimitlessDbShardGroupDns(oldChinaRegionCustomDomain))
	assert.True(t, IsLimitlessDbShardGroupDns(oldChinaRegionLimitlessDbShardGroup))
}
