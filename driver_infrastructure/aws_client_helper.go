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

package driver_infrastructure

import (
	"awssql/error_util"
	"awssql/host_info_util"
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Aws Secrets Manager Client.
type AwsSecretsManagerClient interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

type NewAwsSecretsManagerClientProvider func(hostInfo *host_info_util.HostInfo,
	props map[string]string,
	endpoint string,
	region string) (AwsSecretsManagerClient, error)

func NewAwsSecretsManagerClient(hostInfo *host_info_util.HostInfo,
	props map[string]string,
	endpoint string,
	region string) (AwsSecretsManagerClient, error) {
	awsCredentialsProvider, err := GetAwsCredentialsProvider(*hostInfo, props)

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

// Aws Sts Client.
type AwsStsClient interface {
	AssumeRoleWithSAML(ctx context.Context, params *sts.AssumeRoleWithSAMLInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleWithSAMLOutput, error)
}

type AwsStsClientProvider func(region string) AwsStsClient

func NewAwsStsClient(region string) AwsStsClient {
	options := sts.Options{Region: region}
	return sts.New(options)
}
