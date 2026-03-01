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
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_telemetry "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/util/telemetry"
	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	federated_auth "github.com/aws/aws-advanced-go-wrapper/federated-auth"
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

var federatedAuthUsername = "someFederatedUsername@example.com"
var federatedAuthPassword = "somePassword"
var federatedAuthDbUser = "iamUser"

func setupAdfsMockPluginServiceWithTelemetry(t *testing.T) *mock_driver_infrastructure.MockPluginService {
	ctrl := gomock.NewController(t)
	mockPluginService := mock_driver_infrastructure.NewMockPluginService(ctrl)
	mockTelemetryFactory := mock_telemetry.NewMockTelemetryFactory(ctrl)
	mockTelemetryCtx := mock_telemetry.NewMockTelemetryContext(ctrl)
	ctxBefore := context.Background()

	mockPluginService.EXPECT().GetTelemetryContext().Return(ctxBefore).AnyTimes()
	mockPluginService.EXPECT().GetTelemetryFactory().Return(mockTelemetryFactory).AnyTimes()
	mockPluginService.EXPECT().SetTelemetryContext(gomock.Any()).AnyTimes()
	mockTelemetryFactory.EXPECT().OpenTelemetryContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(mockTelemetryCtx, ctxBefore).AnyTimes()
	mockTelemetryCtx.EXPECT().CloseContext().AnyTimes()
	mockTelemetryCtx.EXPECT().SetSuccess(gomock.Any()).AnyTimes()
	mockTelemetryCtx.EXPECT().SetError(gomock.Any()).AnyTimes()

	return mockPluginService
}

func setupAdfsMockPluginService(t *testing.T) *mock_driver_infrastructure.MockPluginService {
	ctrl := gomock.NewController(t)
	return mock_driver_infrastructure.NewMockPluginService(ctrl)
}

func TestAdfsCredProviderGetSamlAssertion(t *testing.T) {
	getAdfsTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		getReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-sign-in-page.html")))),
		}
		doReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-saml.html")))),
		}
		return MockHttpClient{getReturnValue: &getReturnValue, doReturnValues: []*http.Response{&doReturnValue}, doCallCount: new(int)}
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
		property_util.IDP_USERNAME.Name, federatedAuthUsername,
		property_util.IDP_PASSWORD.Name, federatedAuthPassword,
	)
	mockPluginService := setupAdfsMockPluginServiceWithTelemetry(t)
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, mockPluginService)

	content := readFile(t, "./resources/saml-assertion.txt")
	expectedSamlAssertion := strings.ReplaceAll(strings.ReplaceAll(string(content), "\n", ""), "\r", "")

	samlAssertion, err := adfsCredentialsProviderFactory.GetSamlAssertion(props)
	assert.NoError(t, err)
	assert.Equal(t, expectedSamlAssertion, samlAssertion)
}

func TestAdfsCredProviderGetSignInPageFailure(t *testing.T) {
	getAdfsTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		getReturnValue := http.Response{
			StatusCode: 500,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-sign-in-page.html")))),
		}
		doReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-saml.html")))),
		}
		return MockHttpClient{getReturnValue: &getReturnValue, doReturnValues: []*http.Response{&doReturnValue}, doCallCount: new(int)}
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
		property_util.IDP_USERNAME.Name, federatedAuthUsername,
		property_util.IDP_PASSWORD.Name, federatedAuthPassword,
	)
	mockPluginService := setupAdfsMockPluginServiceWithTelemetry(t)
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, mockPluginService)

	samlAssertion, err := adfsCredentialsProviderFactory.GetSamlAssertion(props)
	assert.Error(t, err)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("AdfsCredentialsProviderFactory.signOnPageRequestFailed", 500)), err)
	assert.Zero(t, samlAssertion)
}

func TestAdfsCredProviderGetSignInPageError(t *testing.T) {
	getAdfsTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		getReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-sign-in-page.html")))),
		}
		doReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-saml.html")))),
		}
		errReturnValue := errors.New("HTTP Error")
		return MockHttpClient{getReturnValue: &getReturnValue, doReturnValues: []*http.Response{&doReturnValue}, doCallCount: new(int), errReturnValue: errReturnValue}
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
		property_util.IDP_USERNAME.Name, federatedAuthUsername,
		property_util.IDP_PASSWORD.Name, federatedAuthPassword,
	)
	mockPluginService := setupAdfsMockPluginServiceWithTelemetry(t)
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, mockPluginService)

	samlAssertion, err := adfsCredentialsProviderFactory.GetSamlAssertion(props)
	assert.Equal(t, errors.New("HTTP Error"), err)
	assert.Zero(t, samlAssertion)
}

func TestAdfsCredProviderGetSamlAssertionLoginFailure(t *testing.T) {
	getAdfsTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		getReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-sign-in-page.html")))),
		}
		doReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-sign-in-page.html")))),
		}
		return MockHttpClient{getReturnValue: &getReturnValue, doReturnValues: []*http.Response{&doReturnValue}, doCallCount: new(int)}
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
		property_util.IDP_USERNAME.Name, federatedAuthUsername,
		property_util.IDP_PASSWORD.Name, federatedAuthPassword,
	)
	mockPluginService := setupAdfsMockPluginServiceWithTelemetry(t)
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, mockPluginService)

	samlAssertion, err := adfsCredentialsProviderFactory.GetSamlAssertion(props)
	assert.Error(t, err)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("AdfsCredentialsProviderFactory.failedLogin")), err)
	assert.Zero(t, samlAssertion)
}

func TestAdfsCredProviderGetSamlAssertionPostFailure(t *testing.T) {
	getAdfsTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		getReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-sign-in-page.html")))),
		}
		doReturnValue := http.Response{
			StatusCode: 500,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-saml.html")))),
		}
		return MockHttpClient{getReturnValue: &getReturnValue, doReturnValues: []*http.Response{&doReturnValue}, doCallCount: new(int)}
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
		property_util.IDP_USERNAME.Name, federatedAuthUsername,
		property_util.IDP_PASSWORD.Name, federatedAuthPassword,
	)
	mockPluginService := setupAdfsMockPluginServiceWithTelemetry(t)
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, mockPluginService)

	samlAssertion, err := adfsCredentialsProviderFactory.GetSamlAssertion(props)
	assert.Error(t, err)
	assert.Equal(t, error_util.NewGenericAwsWrapperError(error_util.GetMessage("AdfsCredentialsProviderFactory.signOnPagePostActionRequestFailed", 500)), err)
	assert.Zero(t, samlAssertion)
}

func TestAdfsCredProviderGetSamlAssertionPostError(t *testing.T) {
	getAdfsTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		getReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-sign-in-page.html")))),
		}
		doReturnValue := http.Response{
			StatusCode: 500,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-saml.html")))),
		}
		errReturnValue := errors.New("HTTP Error")
		return MockHttpClient{getReturnValue: &getReturnValue, doReturnValues: []*http.Response{&doReturnValue}, doCallCount: new(int), errReturnValue: errReturnValue}
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
		property_util.IDP_USERNAME.Name, federatedAuthUsername,
		property_util.IDP_PASSWORD.Name, federatedAuthPassword,
	)
	mockPluginService := setupAdfsMockPluginServiceWithTelemetry(t)
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, mockPluginService)

	samlAssertion, err := adfsCredentialsProviderFactory.GetSamlAssertion(props)
	assert.Equal(t, errors.New("HTTP Error"), err)
	assert.Zero(t, samlAssertion)
}

func TestAdfsCredProviderGetUriAndParametersFromSignInPage(t *testing.T) {
	getAdfsTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		getReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-sign-in-page.html")))),
		}
		doReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-saml.html")))),
		}
		return MockHttpClient{getReturnValue: &getReturnValue, doReturnValues: []*http.Response{&doReturnValue}, doCallCount: new(int)}
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
		property_util.IDP_USERNAME.Name, federatedAuthUsername,
		property_util.IDP_PASSWORD.Name, federatedAuthPassword,
	)
	mockPluginService := setupAdfsMockPluginService(t)
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, mockPluginService)
	uri, params, err := adfsCredentialsProviderFactory.GetUriAndParamsFromSignInPage("https://ec2amaz-ab3cdef.example.com", props)
	assert.Nil(t, err)
	assert.Equal(
		t,
		uri,
		"https://ec2amaz-ab3cdef.example.com:443/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=urn:amazon:webservices&client-request-id=bdfdf240-41c5-4684-ac03-0080010000e8")
	assert.Equal(t, federatedAuthUsername, params["UserName"])
	assert.Equal(t, federatedAuthPassword, params["Password"])
	assert.Equal(t, "true", params["Kmsi"])
	assert.Equal(t, "FormsAuthentication", params["AuthMethod"])
}

func TestAdfsCredProviderMissingInputTags(t *testing.T) {
	getAdfsTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		getReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/empty.html")))),
		}
		doReturnValue := http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/adfs-saml.html")))),
		}
		return MockHttpClient{getReturnValue: &getReturnValue, doReturnValues: []*http.Response{&doReturnValue}, doCallCount: new(int)}
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
		property_util.IDP_USERNAME.Name, federatedAuthUsername,
		property_util.IDP_PASSWORD.Name, federatedAuthPassword,
	)
	mockPluginService := setupAdfsMockPluginService(t)
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, mockPluginService)

	_, params, err := adfsCredentialsProviderFactory.GetUriAndParamsFromSignInPage("https://ec2amaz-ab3cdef.example.com", props)
	assert.Equal(t, 0, len(params))
	assert.Nil(t, err)
}

func TestAdfsCredProviderGetSignInPageUrl(t *testing.T) {
	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
	)
	propsWithUrlScheme := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "federatedAuth",
		property_util.DB_USER.Name, federatedAuthDbUser,
		property_util.IDP_ENDPOINT.Name, "ec2amaz-ab3cdef.example.com",
	)
	adfsCredentialsProviderFactory := &federated_auth.AdfsCredentialsProviderFactory{}

	url1 := adfsCredentialsProviderFactory.GetSignInPageUrl(props)
	assert.Equal(t, "https://ec2amaz-ab3cdef.example.com:443/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=urn:amazon:webservices", url1)

	url2 := adfsCredentialsProviderFactory.GetSignInPageUrl(propsWithUrlScheme)
	assert.Equal(t, "https://ec2amaz-ab3cdef.example.com:443/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=urn:amazon:webservices", url2)
}
