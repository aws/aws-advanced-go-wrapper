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
	"awssql/driver_infrastructure"
	"awssql/plugins/federated_auth"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetAwsCredentialsProviderGetSamlAssertionError(t *testing.T) {
	getSamlAssertionFunc := func(props map[string]string) (string, error) {
		return "", errors.New("something went wrong")
	}
	factory := &federated_auth.SamlCredentialsProviderFactory{AwsStsClientProvider: driver_infrastructure.NewAwsStsClient, GetSamlAssertionFunc: getSamlAssertionFunc}
	_, err := factory.GetAwsCredentialsProvider(federatedAuthHost, federatedAuthRegion, map[string]string{})
	assert.Equal(t, errors.New("something went wrong"), err)
}

func TestGetAwsCredentialsProviderAssumeRoleError(t *testing.T) {
	getSamlAssertionFunc := func(props map[string]string) (string, error) {
		return "saml", nil
	}
	getStsClientFunc := func(region string) driver_infrastructure.AwsStsClient {
		return &MockAwsStsClient{assumeRoleWithSAMLErr: errors.New("something went wrong")}
	}
	factory := &federated_auth.SamlCredentialsProviderFactory{AwsStsClientProvider: getStsClientFunc, GetSamlAssertionFunc: getSamlAssertionFunc}
	_, err := factory.GetAwsCredentialsProvider(federatedAuthHost, federatedAuthRegion, map[string]string{})
	assert.Equal(t, errors.New("something went wrong"), err)
}

func TestFormatIdpEndpoint(t *testing.T) {
	samlCredentialsProviderFactory := federated_auth.SamlCredentialsProviderFactory{}

	endpoint := samlCredentialsProviderFactory.FormatIdpEndpoint("https://localhost")
	assert.Equal(t, "https://localhost", endpoint)

	endpoint = samlCredentialsProviderFactory.FormatIdpEndpoint("localhost")
	assert.Equal(t, "https://localhost", endpoint)
}
