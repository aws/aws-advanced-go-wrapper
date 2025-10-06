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

	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type CredentialsProviderFactory interface {
	GetAwsCredentialsProvider(host string, region region_util.Region, props *utils.RWMap[string, string]) (aws.CredentialsProvider, error)
}

type SamlCredentialsProviderFactory struct {
	AwsStsClientProvider AwsStsClientProvider
	GetSamlAssertionFunc func(props *utils.RWMap[string, string]) (string, error)
}

func (s *SamlCredentialsProviderFactory) GetAwsCredentialsProvider(_ string, region region_util.Region, props *utils.RWMap[string, string]) (aws.CredentialsProvider, error) {
	samlAssertion, err := s.GetSamlAssertionFunc(props)
	if err != nil {
		return nil, err
	}
	client := s.AwsStsClientProvider(string(region))

	idpRoleArn := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_ROLE_ARN)
	iamIdpRoleArn := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_IDP_ARN)
	input := sts.AssumeRoleWithSAMLInput{
		RoleArn:       &idpRoleArn,
		PrincipalArn:  &iamIdpRoleArn,
		SAMLAssertion: &samlAssertion,
	}

	output, err := client.AssumeRoleWithSAML(context.TODO(), &input)
	if output != nil && err == nil {
		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     *output.Credentials.AccessKeyId,
				SecretAccessKey: *output.Credentials.SecretAccessKey,
				SessionToken:    *output.Credentials.SessionToken,
			},
		}))
		if err != nil {
			return nil, err
		}
		return cfg.Credentials, nil
	}
	return nil, err
}

func (s *SamlCredentialsProviderFactory) FormatIdpEndpoint(endpoint string) string {
	// Only add "https://" if user has passed their idpEndpoint without the URL scheme.
	if !strings.HasPrefix(endpoint, "https://") {
		return "https://" + endpoint
	}
	return endpoint
}
