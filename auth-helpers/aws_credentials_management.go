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

package auth_helpers

import (
	"context"
	"sync"

	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type AwsCredentialsProviderHandler interface {
	GetAwsCredentialsProvider(hostInfo host_info_util.HostInfo, props *utils.RWMap[string]) (aws.CredentialsProvider, error)
}

var awsCredentialsProviderHandler AwsCredentialsProviderHandler

var awsCredentialsProviderManageLock sync.RWMutex

func GetAwsCredentialsProvider(hostInfo host_info_util.HostInfo, props *utils.RWMap[string]) (aws.CredentialsProvider, error) {
	awsCredentialsProviderManageLock.RLock()
	defer awsCredentialsProviderManageLock.RUnlock()
	if awsCredentialsProviderHandler != nil {
		return awsCredentialsProviderHandler.GetAwsCredentialsProvider(hostInfo, props)
	}
	return getDefaultProvider(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.AWS_PROFILE))
}

func SetAwsCredentialsProviderHandler(credentialsHandler AwsCredentialsProviderHandler) {
	awsCredentialsProviderManageLock.Lock()
	defer awsCredentialsProviderManageLock.Unlock()
	awsCredentialsProviderHandler = credentialsHandler
}

func getDefaultProvider(profile string) (aws.CredentialsProvider, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithSharedConfigProfile(profile))
	if err != nil {
		return nil, err
	}
	return cfg.Credentials, nil
}
