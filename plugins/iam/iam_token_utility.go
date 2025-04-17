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
	"awssql/region_util"
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
	) (string, error)
}

type RegularIamTokenUtility struct{}

func (iamTokenUtility RegularIamTokenUtility) GenerateAuthenticationToken(
	user string,
	host string,
	port int,
	region region_util.Region,
	awsCredentialsProvider aws.CredentialsProvider,
) (string, error) {
	authToken, err := auth.BuildAuthToken(
		context.TODO(),
		host+":"+strconv.Itoa(port),
		string(region),
		user,
		awsCredentialsProvider,
	)
	if err != nil {
		return "", err
	}
	return authToken, nil
}
