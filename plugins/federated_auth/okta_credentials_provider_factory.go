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

package federated_auth

import (
	"awssql/driver_infrastructure"
	"awssql/error_util"
	"awssql/property_util"
	"awssql/utils"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/PuerkitoBio/goquery"
)

var OKTA_AWS_APP_NAME = "amazon_aws"
var ONE_TIME_TOKEN = "onetimetoken"

type OktaCredentialsProviderFactory struct {
	SamlCredentialsProviderFactory
	httpClientProvider HttpClientProvider
}

func NewOktaCredentialsProviderFactory(
	httpClientProvider HttpClientProvider,
	awsStsClientProvider driver_infrastructure.AwsStsClientProvider,
) *OktaCredentialsProviderFactory {
	providerFactory := &OktaCredentialsProviderFactory{
		httpClientProvider: httpClientProvider,
	}
	providerFactory.GetSamlAssertionFunc = providerFactory.GetSamlAssertion
	providerFactory.AwsStsClientProvider = awsStsClientProvider
	return providerFactory
}

func (o *OktaCredentialsProviderFactory) GetSamlAssertion(props map[string]string) (string, error) {
	sessionToken, err := o.GetSessionToken(props)
	if err != nil {
		return "", err
	}

	httpTimeoutMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.HTTP_TIMEOUT)
	insecureSsl := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.SSL_INSECURE)
	client := o.httpClientProvider(httpTimeoutMs, insecureSsl, nil)

	baseUri, err := getSamlUrl(props)
	if err != nil {
		return "", err
	}

	// Encode parameters for url
	queryParams := url.Values{}
	queryParams.Set(ONE_TIME_TOKEN, sessionToken)
	baseUri.RawQuery = queryParams.Encode()

	req, err := http.NewRequest(http.MethodGet, baseUri.String(), nil)
	if err != nil {
		return "", err
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	statusCode := resp.StatusCode
	if statusCode/100 != 2 {
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("OktaAuthPlugin.httpNon200StatusCode", baseUri.String(), statusCode))
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", err
	}

	samlResponse := ""
	doc.Find(`input[name="SAMLResponse"]`).Each(func(i int, s *goquery.Selection) {
		val, exists := s.Attr("value")
		if exists {
			samlResponse = val
		}
	})

	if samlResponse == "" {
		return samlResponse, error_util.NewGenericAwsWrapperError(error_util.GetMessage("OktaAuthPlugin.failedSamlAssertion", baseUri.String()))
	}
	return samlResponse, nil
}

func (o *OktaCredentialsProviderFactory) GetSessionToken(props map[string]string) (string, error) {
	idpHost := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_ENDPOINT)
	idpUser := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_USERNAME)
	idpPassword := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_PASSWORD)
	httpTimeoutMs := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.HTTP_TIMEOUT)
	insecureSsl := property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.SSL_INSECURE)

	client := o.httpClientProvider(httpTimeoutMs, insecureSsl, nil)

	// Prepare JSON data
	payload := map[string]string{
		"username": idpUser,
		"password": idpPassword,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	sessionTokenEndpoint := "https://" + idpHost + "/api/v1/authn"

	req, err := http.NewRequest(http.MethodPost, sessionTokenEndpoint, bytes.NewBuffer(jsonData))

	if err != nil {
		return "", err
	}

	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if resp == nil || err != nil {
		return "", err
	}
	defer resp.Body.Close()

	statusCode := resp.StatusCode

	if statusCode/100 != 2 {
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("OktaAuthPlugin.httpNon200StatusCode", sessionTokenEndpoint, statusCode))
	}

	var result struct {
		SessionToken string `json:"sessionToken"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return "", err
	}

	if result.SessionToken == "" {
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("OktaAuthPlugin.unableToRetrieveSessionToken"))
	}
	return result.SessionToken, nil
}

func getSamlUrl(props map[string]string) (*url.URL, error) {
	idpHost := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_ENDPOINT)
	appId := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.APP_ID)

	baseUri := fmt.Sprintf("https://%s/app/%s/%s/sso/saml", idpHost, OKTA_AWS_APP_NAME, appId)

	err := utils.ValidateUrl(baseUri)
	if err != nil {
		return nil, err
	}

	return url.Parse(baseUri)
}
