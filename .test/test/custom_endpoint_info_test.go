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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"

	custom_endpoint "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
)

func TestNewCustomEndpointInfo_ValidStaticMembers(t *testing.T) {
	endpoint := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.cluster-custom-xyz.us-east-2.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1", "instance-2"},
		ExcludedMembers:             []string{},
	}

	info, err := custom_endpoint.NewCustomEndpointInfo(endpoint)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	staticMembers := info.GetStaticMembers()
	assert.NotNil(t, staticMembers)
	assert.True(t, staticMembers["instance-1"])
	assert.True(t, staticMembers["instance-2"])

	excludedMembers := info.GetExcludedMembers()
	assert.Nil(t, excludedMembers)
}

func TestNewCustomEndpointInfo_ValidExclusionList(t *testing.T) {
	endpoint := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.cluster-custom-xyz.us-east-2.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("ANY"),
		StaticMembers:               []string{},
		ExcludedMembers:             []string{"instance-3"},
	}

	info, err := custom_endpoint.NewCustomEndpointInfo(endpoint)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	staticMembers := info.GetStaticMembers()
	assert.Nil(t, staticMembers)

	excludedMembers := info.GetExcludedMembers()
	assert.NotNil(t, excludedMembers)
	assert.True(t, excludedMembers["instance-3"])
}

func TestNewCustomEndpointInfo_NilDBClusterEndpointIdentifier(t *testing.T) {
	endpoint := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: nil,
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("ANY"),
	}

	info, err := custom_endpoint.NewCustomEndpointInfo(endpoint)
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestNewCustomEndpointInfo_NilDBClusterIdentifier(t *testing.T) {
	endpoint := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         nil,
		Endpoint:                    aws.String("my-endpoint.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("ANY"),
	}

	info, err := custom_endpoint.NewCustomEndpointInfo(endpoint)
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestNewCustomEndpointInfo_NilEndpoint(t *testing.T) {
	endpoint := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    nil,
		CustomEndpointType:          aws.String("ANY"),
	}

	info, err := custom_endpoint.NewCustomEndpointInfo(endpoint)
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestNewCustomEndpointInfo_EmptyMembers(t *testing.T) {
	endpoint := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{},
		ExcludedMembers:             []string{},
	}

	info, err := custom_endpoint.NewCustomEndpointInfo(endpoint)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	// With no static members, it defaults to EXCLUSION_LIST
	assert.Nil(t, info.GetStaticMembers())
	excludedMembers := info.GetExcludedMembers()
	assert.NotNil(t, excludedMembers)
	assert.Empty(t, excludedMembers)
}

func TestCustomEndpointInfo_Equals_SameInfo(t *testing.T) {
	endpoint := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1", "instance-2"},
	}

	info1, err := custom_endpoint.NewCustomEndpointInfo(endpoint)
	assert.NoError(t, err)
	info2, err := custom_endpoint.NewCustomEndpointInfo(endpoint)
	assert.NoError(t, err)

	assert.True(t, info1.Equals(info2))
}

func TestCustomEndpointInfo_Equals_DifferentMembers(t *testing.T) {
	endpoint1 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}
	endpoint2 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1", "instance-2"},
	}

	info1, _ := custom_endpoint.NewCustomEndpointInfo(endpoint1)
	info2, _ := custom_endpoint.NewCustomEndpointInfo(endpoint2)

	assert.False(t, info1.Equals(info2))
}

func TestCustomEndpointInfo_Equals_DifferentRoleType(t *testing.T) {
	endpoint1 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}
	endpoint2 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("my-endpoint.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("ANY"),
		StaticMembers:               []string{"instance-1"},
	}

	info1, _ := custom_endpoint.NewCustomEndpointInfo(endpoint1)
	info2, _ := custom_endpoint.NewCustomEndpointInfo(endpoint2)

	assert.False(t, info1.Equals(info2))
}

func TestCustomEndpointInfo_Equals_DifferentUrl(t *testing.T) {
	endpoint1 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("url-1.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}
	endpoint2 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("url-2.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}

	info1, _ := custom_endpoint.NewCustomEndpointInfo(endpoint1)
	info2, _ := custom_endpoint.NewCustomEndpointInfo(endpoint2)

	assert.False(t, info1.Equals(info2))
}

func TestCustomEndpointInfo_Equals_DifferentClusterIdentifier(t *testing.T) {
	endpoint1 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("cluster-1"),
		Endpoint:                    aws.String("url.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}
	endpoint2 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("cluster-2"),
		Endpoint:                    aws.String("url.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}

	info1, _ := custom_endpoint.NewCustomEndpointInfo(endpoint1)
	info2, _ := custom_endpoint.NewCustomEndpointInfo(endpoint2)

	assert.False(t, info1.Equals(info2))
}

func TestCustomEndpointInfo_Equals_DifferentEndpointIdentifier(t *testing.T) {
	endpoint1 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("endpoint-1"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("url.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}
	endpoint2 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("endpoint-2"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("url.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}

	info1, _ := custom_endpoint.NewCustomEndpointInfo(endpoint1)
	info2, _ := custom_endpoint.NewCustomEndpointInfo(endpoint2)

	assert.False(t, info1.Equals(info2))
}

func TestCustomEndpointInfo_Equals_StaticVsExclusion(t *testing.T) {
	endpoint1 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("url.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}
	endpoint2 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("url.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		ExcludedMembers:             []string{"instance-1"},
	}

	info1, _ := custom_endpoint.NewCustomEndpointInfo(endpoint1)
	info2, _ := custom_endpoint.NewCustomEndpointInfo(endpoint2)

	assert.False(t, info1.Equals(info2))
}

func TestCustomEndpointInfo_RoleTypeCaseInsensitive(t *testing.T) {
	endpoint1 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("url.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("reader"),
		StaticMembers:               []string{"instance-1"},
	}
	endpoint2 := types.DBClusterEndpoint{
		DBClusterEndpointIdentifier: aws.String("my-endpoint"),
		DBClusterIdentifier:         aws.String("my-cluster"),
		Endpoint:                    aws.String("url.xyz.rds.amazonaws.com"),
		CustomEndpointType:          aws.String("READER"),
		StaticMembers:               []string{"instance-1"},
	}

	info1, _ := custom_endpoint.NewCustomEndpointInfo(endpoint1)
	info2, _ := custom_endpoint.NewCustomEndpointInfo(endpoint2)

	assert.True(t, info1.Equals(info2))
}
