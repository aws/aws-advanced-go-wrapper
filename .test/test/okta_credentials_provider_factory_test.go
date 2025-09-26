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
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/okta"

	"github.com/stretchr/testify/assert"
)

func TestOktaCredentialsProviderGetSamlAssertion(t *testing.T) {
	doReturnValue1 := http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/okta/okta-session-response.json")))),
	}
	doReturnValue2 := http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/okta/okta-saml.html")))),
	}
	mockHttp := &MockHttpClient{getReturnValue: nil, doReturnValues: []*http.Response{&doReturnValue1, &doReturnValue2}, doCallCount: new(int)}
	getOktaTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		return mockHttp
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "okta",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_ENDPOINT.Name, "dev-1234.okta.com",
		property_util.IDP_USERNAME.Name, "oktauser",
		property_util.IDP_PASSWORD.Name, "oktapassword",
		property_util.APP_ID.Name, "myapp",
	)
	oktaCredentialsProviderFactory := okta.NewOktaCredentialsProviderFactory(getOktaTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

	content := readFile(t, "./resources/okta/okta-saml-value-expected.txt")
	expectedSamlAssertion := strings.ReplaceAll(strings.ReplaceAll(string(content), "\n", ""), "\r", "")

	samlAssertion, err := oktaCredentialsProviderFactory.GetSamlAssertion(props)
	assert.NoError(t, err)
	assert.Equal(t, expectedSamlAssertion, samlAssertion)
}

func TestOktaAuthPluginSessionTokenPageError(t *testing.T) {
	doReturnValue1 := http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/okta/okta-session-response.json")))),
	}
	mockHttp := &MockHttpClient{getReturnValue: nil, doReturnValues: []*http.Response{&doReturnValue1}, doCallCount: new(int)}
	getOktaTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		return mockHttp
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "okta",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_ENDPOINT.Name, "dev-1234.okta.com",
		property_util.IDP_USERNAME.Name, "oktauser",
		property_util.IDP_PASSWORD.Name, "oktapassword",
		property_util.APP_ID.Name, "myapp",
	)
	oktaCredentialsProviderFactory := okta.NewOktaCredentialsProviderFactory(getOktaTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

	samlUrl := "https://dev-1234.okta.com/api/v1/authn"
	samlAssertion, err := oktaCredentialsProviderFactory.GetSamlAssertion(props)
	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage("OktaAuthPlugin.httpNon200StatusCode", samlUrl, "500"), err.Error())
	assert.Zero(t, samlAssertion)
}

func TestOktaGetSessionToken(t *testing.T) {
	doReturnValue1 := http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/okta/okta-session-response.json")))),
	}
	mockHttp := &MockHttpClient{getReturnValue: nil, doReturnValues: []*http.Response{&doReturnValue1}, doCallCount: new(int)}
	getOktaTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		return mockHttp
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "okta",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_ENDPOINT.Name, "dev-1234.okta.com",
		property_util.IDP_USERNAME.Name, "oktauser",
		property_util.IDP_PASSWORD.Name, "oktapassword",
		property_util.APP_ID.Name, "myapp",
	)
	oktaCredentialsProviderFactory := okta.NewOktaCredentialsProviderFactory(getOktaTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

	content := readFile(t, "./resources/okta/okta-expected-session-token.txt")
	expectedSessionToken := strings.ReplaceAll(strings.ReplaceAll(string(content), "\n", ""), "\r", "")

	sessionToken, err := oktaCredentialsProviderFactory.GetSessionToken(props)
	assert.NoError(t, err)
	assert.Equal(t, expectedSessionToken, sessionToken)
}

func TestOktaAuthPluginEmptySessionTokenError(t *testing.T) {
	doReturnValue1 := http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader("{\"sessionToken\": \"\"}")),
	}
	mockHttp := &MockHttpClient{getReturnValue: nil, doReturnValues: []*http.Response{&doReturnValue1}, doCallCount: new(int)}
	getOktaTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		return mockHttp
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "okta",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_ENDPOINT.Name, "dev-1234.okta.com",
		property_util.IDP_USERNAME.Name, "oktauser",
		property_util.IDP_PASSWORD.Name, "oktapassword",
		property_util.APP_ID.Name, "myapp",
	)
	oktaCredentialsProviderFactory := okta.NewOktaCredentialsProviderFactory(getOktaTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

	samlAssertion, err := oktaCredentialsProviderFactory.GetSamlAssertion(props)
	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage("OktaAuthPlugin.unableToRetrieveSessionToken"), err.Error())
	assert.Zero(t, samlAssertion)
}

func TestOktaAuthPluginHttpClientError(t *testing.T) {
	doReturnValue1 := http.Response{
		StatusCode: 400,
		Body:       io.NopCloser(strings.NewReader("{\"sessionToken\": \"\"}")),
	}
	mockHttp := &MockHttpClient{getReturnValue: nil, doReturnValues: []*http.Response{&doReturnValue1}, doCallCount: new(int)}
	mockHttp.errReturnValue = errors.New("someHttpError")
	getOktaTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		return mockHttp
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "okta",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_ENDPOINT.Name, "dev-1234.okta.com",
		property_util.IDP_USERNAME.Name, "oktauser",
		property_util.IDP_PASSWORD.Name, "oktapassword",
		property_util.APP_ID.Name, "myapp",
	)
	oktaCredentialsProviderFactory := okta.NewOktaCredentialsProviderFactory(getOktaTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

	samlAssertion, err := oktaCredentialsProviderFactory.GetSamlAssertion(props)
	assert.Error(t, err)
	assert.Equal(t, "someHttpError", err.Error())
	assert.Zero(t, samlAssertion)
}

func TestOktaAuthPluginHttpErrorOnSamlRequest(t *testing.T) {
	doReturnValue1 := http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/okta/okta-session-response.json")))),
	}
	doReturnValue2 := http.Response{
		StatusCode: 400,
		Body:       io.NopCloser(strings.NewReader(string(readFile(t, "./resources/okta/okta-saml.html")))),
	}
	mockHttp := &MockHttpClient{getReturnValue: nil, doReturnValues: []*http.Response{&doReturnValue1, &doReturnValue2}, doCallCount: new(int)}
	getOktaTestHttpClientFunc := func(timeoutMs int, sslInsecure bool, jar http.CookieJar) auth_helpers.HttpClient {
		return mockHttp
	}

	props := MakeMapFromKeysAndVals(
		property_util.DRIVER_PROTOCOL.Name, "postgresql",
		property_util.PLUGINS.Name, "okta",
		property_util.DB_USER.Name, "jane_doe",
		property_util.IDP_ENDPOINT.Name, "dev-1234.okta.com",
		property_util.IDP_USERNAME.Name, "oktauser",
		property_util.IDP_PASSWORD.Name, "oktapassword",
		property_util.APP_ID.Name, "myapp",
	)
	oktaCredentialsProviderFactory := okta.NewOktaCredentialsProviderFactory(getOktaTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

	content := readFile(t, "./resources/okta/okta-expected-session-token.txt")
	expectedSessionToken := strings.ReplaceAll(strings.ReplaceAll(string(content), "\n", ""), "\r", "")

	expectedUri, err := url.Parse("https://dev-1234.okta.com/app/amazon_aws/myapp/sso/saml")
	assert.NoError(t, err)

	queryParams := url.Values{}
	queryParams.Set(okta.ONE_TIME_TOKEN, expectedSessionToken)
	expectedUri.RawQuery = queryParams.Encode()

	samlAssertion, err := oktaCredentialsProviderFactory.GetSamlAssertion(props)

	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage("OktaAuthPlugin.httpNon200StatusCode", expectedUri.String(), 400), err.Error())
	assert.Zero(t, samlAssertion)
}
