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
	"strings"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/federated-auth"

	"github.com/stretchr/testify/assert"
)

var federatedAuthUsername = "someFederatedUsername@example.com"
var federatedAuthPassword = "somePassword"

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
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

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
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

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
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

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
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

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
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

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
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

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
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))
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
	adfsCredentialsProviderFactory := federated_auth.NewAdfsCredentialsProviderFactory(getAdfsTestHttpClientFunc, NewMockAwsStsClient, CreateMockPluginService(props))

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
