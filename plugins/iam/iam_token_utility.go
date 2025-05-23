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

package iam

import (
	"awssql/driver_infrastructure"
	"awssql/region_util"
	"awssql/utils/telemetry"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"strconv"
)

type IamTokenUtility interface {
	GenerateAuthenticationToken(
		username string,
		host string,
		port int,
		region region_util.Region,
		awsCredentialsProvider aws.CredentialsProvider,
		pluginService driver_infrastructure.PluginService,
	) (string, error)
}

type RegularIamTokenUtility struct{}

func (iamTokenUtility RegularIamTokenUtility) GenerateAuthenticationToken(
	user string,
	host string,
	port int,
	region region_util.Region,
	awsCredentialsProvider aws.CredentialsProvider,
	pluginService driver_infrastructure.PluginService,
) (string, error) {
	parentCtx := pluginService.GetTelemetryContext()
	telemetryCtx, ctx := pluginService.GetTelemetryFactory().OpenTelemetryContext(telemetry.TELEMETRY_FETCH_TOKEN, telemetry.NESTED, parentCtx)
	pluginService.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		pluginService.SetTelemetryContext(parentCtx)
	}()

	authToken, err := auth.BuildAuthToken(
		context.TODO(),
		host+":"+strconv.Itoa(port),
		string(region),
		user,
		awsCredentialsProvider,
	)
	if err != nil {
		telemetryCtx.SetSuccess(false)
		telemetryCtx.SetError(err)
		return "", err
	}

	telemetryCtx.SetSuccess(true)
	return authToken, nil
}
