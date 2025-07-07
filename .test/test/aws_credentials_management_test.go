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
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
)

type mockCredentialsProviderHandler struct {
	provider aws.CredentialsProvider
	called   bool
}

func (m *mockCredentialsProviderHandler) GetAwsCredentialsProvider(hostInfo host_info_util.HostInfo, props map[string]string) (aws.CredentialsProvider, error) {
	m.called = true
	return m.provider, nil
}

type staticProvider struct {
	creds aws.Credentials
}

func (s *staticProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return s.creds, nil
}

func resetAwsCredProviderHandler() {
	auth_helpers.SetAwsCredentialsProviderHandler(nil)
}

func Test_GetAwsCredentialsProvider_WithCustomHandler(t *testing.T) {
	resetAwsCredProviderHandler()

	expectedCreds := aws.Credentials{
		AccessKeyID:     "mock-access-key",
		SecretAccessKey: "mock-secret-key",
		Source:          "mock",
	}
	mockProvider := &staticProvider{creds: expectedCreds}
	mockHandler := &mockCredentialsProviderHandler{
		provider: mockProvider,
	}

	auth_helpers.SetAwsCredentialsProviderHandler(mockHandler)

	provider, err := auth_helpers.GetAwsCredentialsProvider(host_info_util.HostInfo{}, map[string]string{})

	assert.NoError(t, err)
	assert.NotNil(t, provider)

	creds, err := provider.Retrieve(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, expectedCreds.AccessKeyID, creds.AccessKeyID)
	assert.True(t, mockHandler.called)
	resetAwsCredProviderHandler()
}

func Test_GetAwsCredentialsProvider_DefaultProvider_InvalidProfile(t *testing.T) {
	resetAwsCredProviderHandler()

	profileName := "nonexistent-profile"
	props := map[string]string{
		property_util.AWS_PROFILE.Name: profileName,
	}

	provider, err := auth_helpers.GetAwsCredentialsProvider(host_info_util.HostInfo{}, props)

	assert.Error(t, err)
	assert.Nil(t, provider)

	// assert that the error contains the profile name that was given
	// to verify that the profile in awsProfile was being used
	assert.True(t, strings.Contains(err.Error(), profileName),
		"expected error message to contain profile name %q, but got: %s", profileName, err.Error())
	resetAwsCredProviderHandler()
}

func Test_GetAwsCredentialsProvider_DefaultProvider_EmptyProfile(t *testing.T) {
	resetAwsCredProviderHandler()

	props := map[string]string{}

	// default credentials should still be valid even if cred file does not exist
	// it will be populated with empty credentials
	provider, err := auth_helpers.GetAwsCredentialsProvider(host_info_util.HostInfo{}, props)
	assert.NoError(t, err)
	assert.NotNil(t, provider)
}
