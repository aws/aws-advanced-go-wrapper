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
	"errors"

	"github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"

	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAwsCredentialsProviderGetSamlAssertionError(t *testing.T) {
	getSamlAssertionFunc := func(props *utils.RWMap[string]) (string, error) {
		return "", errors.New("something went wrong")
	}
	factory := &auth_helpers.SamlCredentialsProviderFactory{AwsStsClientProvider: NewMockAwsStsClient, GetSamlAssertionFunc: getSamlAssertionFunc}
	_, err := factory.GetAwsCredentialsProvider(federatedAuthHost, federatedAuthRegion, emptyProps)
	assert.Equal(t, errors.New("something went wrong"), err)
}

func TestGetAwsCredentialsProviderAssumeRoleError(t *testing.T) {
	getSamlAssertionFunc := func(props *utils.RWMap[string]) (string, error) {
		return "saml", nil
	}
	getStsClientFunc := func(region string) auth_helpers.AwsStsClient {
		return &MockAwsStsClient{assumeRoleWithSAMLErr: errors.New("something went wrong")}
	}
	factory := &auth_helpers.SamlCredentialsProviderFactory{AwsStsClientProvider: getStsClientFunc, GetSamlAssertionFunc: getSamlAssertionFunc}
	_, err := factory.GetAwsCredentialsProvider(federatedAuthHost, federatedAuthRegion, emptyProps)
	assert.Equal(t, errors.New("something went wrong"), err)
}

func TestFormatIdpEndpoint(t *testing.T) {
	samlCredentialsProviderFactory := auth_helpers.SamlCredentialsProviderFactory{}

	endpoint := samlCredentialsProviderFactory.FormatIdpEndpoint("https://localhost")
	assert.Equal(t, "https://localhost", endpoint)

	endpoint = samlCredentialsProviderFactory.FormatIdpEndpoint("localhost")
	assert.Equal(t, "https://localhost", endpoint)
}
