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

package okta

import (
	"database/sql/driver"
	"github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	awssql "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
	"log/slog"
	"time"
)

func init() {
	awssql.UsePluginFactory("okta", NewOktaAuthPluginFactory())
}

type OktaAuthPluginFactory struct{}

func (o OktaAuthPluginFactory) GetInstance(
	pluginService driver_infrastructure.PluginService,
	props map[string]string,
) (driver_infrastructure.ConnectionPlugin, error) {
	providerFactory := NewOktaCredentialsProviderFactory(auth_helpers.GetBasicHttpClient, auth_helpers.NewAwsStsClient, pluginService)

	return NewOktaAuthPlugin(pluginService, providerFactory, auth_helpers.RegularIamTokenUtility{})
}

func (o OktaAuthPluginFactory) ClearCaches() {
	OktaTokenCache.Clear()
}

func NewOktaAuthPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return OktaAuthPluginFactory{}
}

var OktaTokenCache = utils.NewCache[string]()

type OktaAuthPlugin struct {
	pluginService              driver_infrastructure.PluginService
	credentialsProviderFactory auth_helpers.CredentialsProviderFactory
	iamTokenUtility            auth_helpers.IamTokenUtility
	fetchTokenCounter          telemetry.TelemetryCounter
	plugins.BaseConnectionPlugin
}

func NewOktaAuthPlugin(
	pluginService driver_infrastructure.PluginService,
	credentialsProviderFactory auth_helpers.CredentialsProviderFactory,
	iamTokenUtility auth_helpers.IamTokenUtility) (*OktaAuthPlugin, error) {
	fetchTokenCounter, err := pluginService.GetTelemetryFactory().CreateCounter("oktaAuth.fetchToken.count")
	if err != nil {
		return nil, err
	}
	return &OktaAuthPlugin{
		pluginService:              pluginService,
		credentialsProviderFactory: credentialsProviderFactory,
		iamTokenUtility:            iamTokenUtility,
		fetchTokenCounter:          fetchTokenCounter,
	}, nil
}

func (o *OktaAuthPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD, plugin_helpers.FORCE_CONNECT_METHOD}
}

func (o *OktaAuthPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return o.connectInternal(hostInfo, props, connectFunc)
}

func (o *OktaAuthPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return o.connectInternal(hostInfo, props, connectFunc)
}

func (o *OktaAuthPlugin) connectInternal(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	utils.CheckIdpCredentialsWithFallback(property_util.IDP_USERNAME, property_util.IDP_PASSWORD, props)
	host := auth_helpers.GetIamHost(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_HOST), *hostInfo)
	port := auth_helpers.GetIamPort(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_DEFAULT_PORT),
		*hostInfo,
		o.pluginService.GetDialect().GetDefaultPort())
	region := region_util.GetRegion(host, props, property_util.IAM_REGION)

	if region == "" {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("OktaAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name))
	}

	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		region)

	token, isCachedToken := OktaTokenCache.Get(cacheKey)

	if isCachedToken {
		slog.Debug(error_util.GetMessage("AuthenticationToken.useCachedToken", token))
		property_util.PASSWORD.Set(props, token)
	} else {
		err := o.updateAuthenticationToken(props, region, cacheKey, host, port)

		if err != nil {
			return nil, err
		}
	}

	props[property_util.USER.Name] = property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER)

	conn, err := connectFunc()

	if err != nil && o.pluginService.IsLoginError(err) && isCachedToken {
		err = o.updateAuthenticationToken(props, region, cacheKey, host, port)
		if err != nil {
			return nil, err
		}
		return connectFunc()
	}

	return conn, err
}

func (o *OktaAuthPlugin) updateAuthenticationToken(
	props map[string]string,
	region region_util.Region,
	cacheKey string,
	host string,
	port int) error {
	tokenExpirationSec := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_TOKEN_EXPIRATION)
	credentialsProvider, err := o.credentialsProviderFactory.GetAwsCredentialsProvider(host, region, props)

	if err != nil {
		return err
	}

	o.fetchTokenCounter.Inc(o.pluginService.GetTelemetryContext())
	token, err := o.iamTokenUtility.GenerateAuthenticationToken(property_util.DB_USER.Get(props), host, port, region, credentialsProvider, o.pluginService)
	if err != nil {
		return err
	}
	slog.Debug(error_util.GetMessage("AuthenticationToken.generatedNewToken", token))
	props[property_util.PASSWORD.Name] = token
	OktaTokenCache.Put(cacheKey, token, time.Second*time.Duration(tokenExpirationSec))
	return nil
}
