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

package aws_secrets_manager

import (
	"context"
	"log/slog"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// Aws Secrets Manager Client.
type AwsSecretsManagerClient interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

type NewAwsSecretsManagerClientProvider func(hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	endpoint string,
	region string) (AwsSecretsManagerClient, error)

func NewAwsSecretsManagerClient(hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	endpoint string,
	region string) (AwsSecretsManagerClient, error) {
	awsCredentialsProvider, err := auth_helpers.GetAwsCredentialsProvider(*hostInfo, props.GetAllEntries())

	if err != nil {
		slog.Error(error_util.GetMessage("AwsClientHelper.errorGettingAwsCredentialsProvider"))
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(awsCredentialsProvider))

	if err != nil {
		slog.Error(error_util.GetMessage("AwsClientHelper.errorGettingClientConfig"))
		return nil, err
	}

	baseEndpoint := &endpoint
	if endpoint == "" {
		baseEndpoint = nil
	}

	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.BaseEndpoint = baseEndpoint
		o.Region = region
	})

	return client, err
}
