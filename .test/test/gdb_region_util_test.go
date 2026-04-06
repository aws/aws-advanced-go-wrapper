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
	"context"
	"errors"
	"testing"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
)

type MockRdsDescribeGlobalClustersClient struct {
	Output *rds.DescribeGlobalClustersOutput
	Err    error
}

func (m *MockRdsDescribeGlobalClustersClient) DescribeGlobalClusters(
	_ context.Context,
	_ *rds.DescribeGlobalClustersInput,
	_ ...func(*rds.Options),
) (*rds.DescribeGlobalClustersOutput, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return m.Output, nil
}

func mockRdsClientProvider(client auth_helpers.RdsDescribeGlobalClustersClient) auth_helpers.RdsClientProvider {
	return func(_ aws.CredentialsProvider) auth_helpers.RdsDescribeGlobalClustersClient {
		return client
	}
}

func TestGdbRegionProvider_GetRegion_PropsOverride(t *testing.T) {
	// Props override should skip RDS call
	props := MakeMapFromKeysAndVals(
		property_util.IAM_REGION.Name, "us-west-2",
	)

	provider := auth_helpers.NewGdbRegionProvider(nil)
	provider.RdsClientProvider = mockRdsClientProvider(&MockRdsDescribeGlobalClustersClient{
		Err: errors.New("should not be called"),
	})

	region, err := provider.GetRegion("my-gdb.global-abc123.global.rds.amazonaws.com", props, property_util.IAM_REGION)

	assert.Nil(t, err)
	assert.Equal(t, region_util.US_WEST_2, region)
}

func TestGdbRegionProvider_GetRegion_WriterClusterArn(t *testing.T) {
	// Extract region from writer cluster ARN
	mockClient := &MockRdsDescribeGlobalClustersClient{
		Output: &rds.DescribeGlobalClustersOutput{
			GlobalClusters: []rdsTypes.GlobalCluster{
				{
					GlobalClusterMembers: []rdsTypes.GlobalClusterMember{
						{
							DBClusterArn: aws.String("arn:aws:rds:us-east-2:123456789012:cluster:my-cluster"),
							IsWriter:     aws.Bool(true),
						},
					},
				},
			},
		},
	}

	provider := &auth_helpers.GdbRegionProvider{
		CredentialsProvider: aws.AnonymousCredentials{},
		RdsClientProvider:   mockRdsClientProvider(mockClient),
	}

	region, err := provider.GetRegion("my-gdb.global-abc123.global.rds.amazonaws.com", emptyProps, property_util.IAM_REGION)

	assert.Nil(t, err)
	assert.Equal(t, region_util.US_EAST_2, region)
}

func TestGdbRegionProvider_GetRegion_MultipleMembers_FindsWriter(t *testing.T) {
	// Multiple members: only writer's region returned
	mockClient := &MockRdsDescribeGlobalClustersClient{
		Output: &rds.DescribeGlobalClustersOutput{
			GlobalClusters: []rdsTypes.GlobalCluster{
				{
					GlobalClusterMembers: []rdsTypes.GlobalClusterMember{
						{
							DBClusterArn: aws.String("arn:aws:rds:us-west-1:123456789012:cluster:reader-cluster"),
							IsWriter:     aws.Bool(false),
						},
						{
							DBClusterArn: aws.String("arn:aws:rds:eu-west-1:123456789012:cluster:writer-cluster"),
							IsWriter:     aws.Bool(true),
						},
						{
							DBClusterArn: aws.String("arn:aws:rds:ap-southeast-1:123456789012:cluster:another-reader"),
							IsWriter:     aws.Bool(false),
						},
					},
				},
			},
		},
	}

	provider := &auth_helpers.GdbRegionProvider{
		CredentialsProvider: aws.AnonymousCredentials{},
		RdsClientProvider:   mockRdsClientProvider(mockClient),
	}

	region, err := provider.GetRegion("my-gdb.global-abc123.global.rds.amazonaws.com", emptyProps, property_util.IAM_REGION)

	assert.Nil(t, err)
	assert.Equal(t, region_util.EU_WEST_1, region)
}

func TestGdbRegionProvider_GetRegion_NoWriterFound(t *testing.T) {
	// No IsWriter=true member should error
	mockClient := &MockRdsDescribeGlobalClustersClient{
		Output: &rds.DescribeGlobalClustersOutput{
			GlobalClusters: []rdsTypes.GlobalCluster{
				{
					GlobalClusterMembers: []rdsTypes.GlobalClusterMember{
						{
							DBClusterArn: aws.String("arn:aws:rds:us-west-1:123456789012:cluster:reader-cluster"),
							IsWriter:     aws.Bool(false),
						},
					},
				},
			},
		},
	}

	provider := &auth_helpers.GdbRegionProvider{
		CredentialsProvider: aws.AnonymousCredentials{},
		RdsClientProvider:   mockRdsClientProvider(mockClient),
	}

	region, err := provider.GetRegion("my-gdb.global-abc123.global.rds.amazonaws.com", emptyProps, property_util.IAM_REGION)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "No writer cluster found")
	assert.Equal(t, region_util.Region(""), region)
}

func TestGdbRegionProvider_GetRegion_RdsApiError(t *testing.T) {
	// API error should propagate
	apiErr := errors.New("operation error RDS: DescribeGlobalClusters, access denied")
	mockClient := &MockRdsDescribeGlobalClustersClient{
		Err: apiErr,
	}

	provider := &auth_helpers.GdbRegionProvider{
		CredentialsProvider: aws.AnonymousCredentials{},
		RdsClientProvider:   mockRdsClientProvider(mockClient),
	}

	region, err := provider.GetRegion("my-gdb.global-abc123.global.rds.amazonaws.com", emptyProps, property_util.IAM_REGION)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "writer cluster ARN")
	assert.Contains(t, err.Error(), "access denied")
	assert.Equal(t, region_util.Region(""), region)
}

func TestGdbRegionProvider_GetRegion_NonGlobalHost(t *testing.T) {
	// Non-global hostname should error
	provider := &auth_helpers.GdbRegionProvider{
		CredentialsProvider: aws.AnonymousCredentials{},
		RdsClientProvider: mockRdsClientProvider(&MockRdsDescribeGlobalClustersClient{
			Err: errors.New("should not be called"),
		}),
	}

	region, err := provider.GetRegion("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", emptyProps, property_util.IAM_REGION)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Unable to parse global cluster ID")
	assert.Equal(t, region_util.Region(""), region)
}

func TestGdbRegionProvider_GetRegion_NilCredentials_LazyResolution(t *testing.T) {
	// Nil credentials should be lazily resolved; mock client verifies we get past nil check
	mockClient := &MockRdsDescribeGlobalClustersClient{
		Output: &rds.DescribeGlobalClustersOutput{
			GlobalClusters: []rdsTypes.GlobalCluster{
				{
					GlobalClusterMembers: []rdsTypes.GlobalClusterMember{
						{
							DBClusterArn: aws.String("arn:aws:rds:ap-northeast-1:123456789012:cluster:writer"),
							IsWriter:     aws.Bool(true),
						},
					},
				},
			},
		},
	}

	provider := &auth_helpers.GdbRegionProvider{
		CredentialsProvider: nil,
		RdsClientProvider:   mockRdsClientProvider(mockClient),
	}

	region, err := provider.GetRegion("my-gdb.global-abc123.global.rds.amazonaws.com", emptyProps, property_util.IAM_REGION)

	// May fail if default credentials unavailable in test env
	if err != nil {
		assert.Contains(t, err.Error(), "credentials")
	} else {
		assert.Equal(t, region_util.AP_NORTHEAST_1, region)
	}
}

func TestGdbRegionProvider_GetRegion_EmptyClusters(t *testing.T) {
	// Empty cluster list should error
	mockClient := &MockRdsDescribeGlobalClustersClient{
		Output: &rds.DescribeGlobalClustersOutput{
			GlobalClusters: []rdsTypes.GlobalCluster{},
		},
	}

	provider := &auth_helpers.GdbRegionProvider{
		CredentialsProvider: aws.AnonymousCredentials{},
		RdsClientProvider:   mockRdsClientProvider(mockClient),
	}

	region, err := provider.GetRegion("my-gdb.global-abc123.global.rds.amazonaws.com", emptyProps, property_util.IAM_REGION)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "No writer cluster found")
	assert.Equal(t, region_util.Region(""), region)
}

func TestDefaultRegionProvider_GetRegion_FromHost(t *testing.T) {
	provider := &region_util.DefaultRegionProvider{}
	region, err := provider.GetRegion("database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com", emptyProps, property_util.IAM_REGION)

	assert.Nil(t, err)
	assert.Equal(t, region_util.US_EAST_2, region)
}

func TestDefaultRegionProvider_GetRegion_FromProps(t *testing.T) {
	provider := &region_util.DefaultRegionProvider{}
	props := MakeMapFromKeysAndVals(
		property_util.IAM_REGION.Name, "eu-central-1",
	)
	region, err := provider.GetRegion("", props, property_util.IAM_REGION)

	assert.Nil(t, err)
	assert.Equal(t, region_util.EU_CENTRAL_1, region)
}

func TestDefaultRegionProvider_GetRegion_Empty(t *testing.T) {
	provider := &region_util.DefaultRegionProvider{}
	region, err := provider.GetRegion("", emptyProps, property_util.IAM_REGION)

	assert.Nil(t, err)
	assert.Equal(t, region_util.Region(""), region)
}

func TestGetRegionFromArn(t *testing.T) {
	assert.Equal(t, region_util.US_EAST_2, region_util.GetRegionFromArn("arn:aws:rds:us-east-2:123456789012:cluster:my-cluster"))
	assert.Equal(t, region_util.EU_WEST_1, region_util.GetRegionFromArn("arn:aws:rds:eu-west-1:123456789012:cluster:my-cluster"))
	assert.Equal(t, region_util.AP_SOUTHEAST_1, region_util.GetRegionFromArn("arn:aws:rds:ap-southeast-1:123456789012:cluster:my-cluster"))
	assert.Equal(t, region_util.Region(""), region_util.GetRegionFromArn("not-an-arn"))
	assert.Equal(t, region_util.Region(""), region_util.GetRegionFromArn(""))
	assert.Equal(t, region_util.Region(""), region_util.GetRegionFromArn("arn:aws:rds:invalid-region:123456789012:cluster:my-cluster"))
}
