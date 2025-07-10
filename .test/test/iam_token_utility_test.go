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

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestGenerateAuthenticationToken_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTelemetryFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryContext := mock_telemetry.NewMockTelemetryContext(ctrl)

	parentCtx := context.WithValue(context.TODO(), "key", "value")
	mockPluginService.EXPECT().GetTelemetryContext().Return(parentCtx)
	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockTelemetryFactory.EXPECT().OpenTelemetryContext(gomock.Any(), gomock.Any(), parentCtx).
		Return(mockTelemetryContext, context.TODO())

	mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).Times(2)
	mockTelemetryContext.EXPECT().SetSuccess(true)
	mockTelemetryContext.EXPECT().CloseContext()

	// Stub out auth.BuildAuthToken temporarily
	mockBuildTokenFunc :=
		func(ctx context.Context,
			endpoint string,
			region string,
			dbUser string,
			creds aws.CredentialsProvider,
			optFns ...func(options *auth.BuildAuthTokenOptions)) (string, error) {
			return "test-token", nil
		}
	utility := auth_helpers.RegularIamTokenUtility{}
	utility.SetBuildAuthTokenFunc(mockBuildTokenFunc)

	token, err := utility.GenerateAuthenticationToken(
		"testuser", "localhost", 3306, region_util.Region("us-west-2"), nil, mockPluginService,
	)

	assert.NoError(t, err)
	assert.Equal(t, "test-token", token)
}

func TestGenerateAuthenticationToken_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTelemetryFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryContext := mock_telemetry.NewMockTelemetryContext(ctrl)

	parentCtx := context.WithValue(context.TODO(), "key", "value")
	mockPluginService.EXPECT().GetTelemetryContext().Return(parentCtx)
	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory)
	mockTelemetryFactory.EXPECT().OpenTelemetryContext(gomock.Any(), gomock.Any(), parentCtx).
		Return(mockTelemetryContext, context.TODO())

	mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).Times(2)
	mockTelemetryContext.EXPECT().SetSuccess(false)
	mockTelemetryContext.EXPECT().SetError(gomock.Any())
	mockTelemetryContext.EXPECT().CloseContext()

	mockBuildTokenFunc :=
		func(ctx context.Context,
			endpoint string,
			region string,
			dbUser string,
			creds aws.CredentialsProvider,
			optFns ...func(options *auth.BuildAuthTokenOptions)) (string, error) {
			return "test-token", errors.New("auth-errors")
		}

	utility := auth_helpers.RegularIamTokenUtility{}
	utility.SetBuildAuthTokenFunc(mockBuildTokenFunc)

	token, err := utility.GenerateAuthenticationToken(
		"testuser", "localhost", 3306, region_util.Region("us-west-2"), nil, mockPluginService,
	)

	assert.Error(t, err)
	assert.Empty(t, token)
}
