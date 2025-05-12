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
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type AdfsCredentialsProviderFactory struct {
	SamlCredentialsProviderFactory
	httpClientProvider HttpClientProvider
}

func NewAdfsCredentialsProviderFactory(
	httpClientProvider HttpClientProvider,
	awsStsClientProvider driver_infrastructure.AwsStsClientProvider) *AdfsCredentialsProviderFactory {
	providerFactory := &AdfsCredentialsProviderFactory{}
	providerFactory.GetSamlAssertionFunc = providerFactory.GetSamlAssertion
	providerFactory.httpClientProvider = httpClientProvider
	providerFactory.AwsStsClientProvider = awsStsClientProvider
	return providerFactory
}

func (a *AdfsCredentialsProviderFactory) GetSamlAssertion(props map[string]string) (string, error) {
	signInPageUrl := a.GetSignInPageUrl(props)
	uri, params, err := a.GetUriAndParamsFromSignInPage(signInPageUrl, props)
	if err != nil {
		return "", err
	}

	samlAssertion, err := a.getSamlAssertionFromPost(uri, params, props)
	if err != nil {
		return "", err
	}

	return samlAssertion, nil
}

func (a *AdfsCredentialsProviderFactory) GetSignInPageUrl(props map[string]string) string {
	return a.FormatIdpEndpoint(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_ENDPOINT)) +
		":" +
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_PORT) +
		"/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=" +
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.RELAYING_PARTY_ID)
}

func (a *AdfsCredentialsProviderFactory) GetUriAndParamsFromSignInPage(uri string, props map[string]string) (string, map[string]string, error) {
	slog.Debug(error_util.GetMessage("AdfsCredentialsProviderFactory.signOnPageUrl", uri))
	err := utils.ValidateUrl(uri)
	if err != nil {
		return "", nil, err
	}

	client := a.httpClientProvider(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.HTTP_TIMEOUT),
		property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.SSL_INSECURE),
		nil)

	resp, err := client.Get(uri)

	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	statusCode := resp.StatusCode
	if statusCode/100 != 2 {
		return "", nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("AdfsCredentialsProviderFactory.signOnPageRequestFailed", statusCode))
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", nil, err
	}
	selection := doc.Find(`form`).First()
	action, _ := selection.Attr("action")

	postUri := uri
	if strings.HasPrefix(action, "/") {
		postUri = a.getFormActionUrl(props, action)
	}

	var params = map[string]string{}

	doc.Find("input").Each(func(i int, s *goquery.Selection) {
		name, exists := s.Attr("name")
		if exists {
			if strings.ToLower(name) == "username" {
				params[name] = property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_USERNAME)
			} else if strings.ToLower(name) == "password" {
				params[name] = property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_PASSWORD)
			} else {
				value, exists := s.Attr("value")
				if exists {
					params[name] = value
				}
			}
		}
	})

	return postUri, params, err
}

func (a *AdfsCredentialsProviderFactory) getFormActionUrl(props map[string]string, action string) string {
	return a.FormatIdpEndpoint(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_ENDPOINT)) +
		":" +
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_PORT) +
		action
}

func (a *AdfsCredentialsProviderFactory) getSamlAssertionFromPost(uri string, params map[string]string, props map[string]string) (string, error) {
	slog.Debug(error_util.GetMessage("AdfsCredentialsProviderFactory.signOnPagePostActionUrl", uri))
	err := utils.ValidateUrl(uri)
	if err != nil {
		return "", err
	}
	values := url.Values{}
	for key, value := range params {
		values.Set(key, value)
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		return "", err
	}
	client := a.httpClientProvider(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.HTTP_TIMEOUT),
		property_util.GetVerifiedWrapperPropertyValue[bool](props, property_util.SSL_INSECURE),
		jar)

	req, _ := http.NewRequest(http.MethodPost, uri, strings.NewReader(values.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)

	if resp == nil || err != nil {
		return "", err
	}
	defer resp.Body.Close()

	statusCode := resp.StatusCode
	if statusCode/100 != 2 {
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("AdfsCredentialsProviderFactory.signOnPagePostActionRequestFailed", statusCode))
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", err
	}
	selection := doc.Find(`input[name="SAMLResponse"]`).First()
	samlAssertion, ok := selection.Attr("value")
	if samlAssertion == "" || !ok {
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("AdfsCredentialsProviderFactory.failedLogin"))
	}

	return samlAssertion, nil
}
