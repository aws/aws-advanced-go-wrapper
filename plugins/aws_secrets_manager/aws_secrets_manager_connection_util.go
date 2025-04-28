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
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/host_info_util"
	"awssql/property_util"
	"awssql/region_util"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type AwsRdsSecrets struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func getCacheKey(
	secretId string,
	region string,
) string {
	return fmt.Sprintf("%s:%s", secretId, region)
}

func getRdsSecretFromAwsSecretsManager(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	endpoint string,
	region string,
	clientProvider driver_infrastructure.NewAwsSecretsManagerClientProvider,
) (AwsRdsSecrets, error) {
	var secret AwsRdsSecrets

	svc, err := clientProvider(
		hostInfo,
		props,
		endpoint,
		region,
	)

	if err != nil {
		slog.Error(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.unableToCreateAwsSecretsManagerClient", err))
		return secret, err
	}

	secretId := props[property_util.SECRETS_MANAGER_SECRET_ID.Name]
	// Get the secret value
	secretOutput, err := svc.GetSecretValue(context.TODO(), &secretsmanager.GetSecretValueInput{
		SecretId: &secretId,
	})

	if err != nil {
		slog.Error(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.unableToGetSecretValue", err))
		return secret, err
	}

	// Parse the secret string
	err = json.Unmarshal([]byte(*secretOutput.SecretString), &secret)

	if err != nil {
		slog.Error(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.unableToParseSecretValue", err))
	}

	return secret, err
}

func GetAwsSecretsManagerHost(awsSecretsManagerHost string, hostInfo host_info_util.HostInfo) string {
	if awsSecretsManagerHost != "" {
		return awsSecretsManagerHost
	}
	return hostInfo.Host
}

func GetAwsSecretsManagerRegion(regionStr string, secretId string) (region_util.Region, error) {
	region := region_util.GetRegionFromRegionString(regionStr)
	if region != "" {
		return region, nil
	}

	region = region_util.GetRegionFromArn(secretId)
	if region == "" {
		return region, errors.New(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.unableToDetermineRegion", property_util.SECRETS_MANAGER_REGION.Name))
	}

	return region, nil
}
