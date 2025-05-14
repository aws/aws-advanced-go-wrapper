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
	"awssql/host_info_util"
	"awssql/plugin_helpers"
	"awssql/plugins"
	"awssql/plugins/iam"
	"awssql/property_util"
	"awssql/region_util"
	"awssql/utils"
	"database/sql/driver"
	"log/slog"
	"time"
)

var TokenCache = utils.NewCache[string]()

type FederatedAuthPluginFactory struct{}

func (f FederatedAuthPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService, props map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	providerFactory := NewAdfsCredentialsProviderFactory(GetBasicHttpClient, driver_infrastructure.NewAwsStsClient)
	return NewFederatedAuthPlugin(pluginService, providerFactory, iam.RegularIamTokenUtility{}), nil
}

func NewFederatedAuthPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return FederatedAuthPluginFactory{}
}

type FederatedAuthPlugin struct {
	pluginService              driver_infrastructure.PluginService
	credentialsProviderFactory CredentialsProviderFactory
	iamTokenUtility            iam.IamTokenUtility
	plugins.BaseConnectionPlugin
}

func NewFederatedAuthPlugin(
	pluginService driver_infrastructure.PluginService,
	providerFactory CredentialsProviderFactory,
	iamTokenUtility iam.IamTokenUtility) *FederatedAuthPlugin {
	return &FederatedAuthPlugin{pluginService: pluginService, credentialsProviderFactory: providerFactory, iamTokenUtility: iamTokenUtility}
}

func (f *FederatedAuthPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD, plugin_helpers.FORCE_CONNECT_METHOD}
}

func (f *FederatedAuthPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return f.connectInternal(hostInfo, props, connectFunc)
}

func (f *FederatedAuthPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return f.connectInternal(hostInfo, props, connectFunc)
}

func (f *FederatedAuthPlugin) connectInternal(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	utils.CheckIdpCredentialsWithFallback(property_util.IDP_USERNAME, property_util.IDP_PASSWORD, props)

	host := iam.GetIamHost(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_HOST), *hostInfo)

	port := iam.GetIamPort(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_DEFAULT_PORT),
		*hostInfo,
		f.pluginService.GetDialect().GetDefaultPort())

	region := region_util.GetRegion(host, props, property_util.IAM_REGION)
	if region == "" {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("FederatedAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name))
	}

	cacheKey := iam.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		region)

	token, isCachedToken := TokenCache.Get(cacheKey)

	if isCachedToken {
		slog.Debug(error_util.GetMessage("AuthenticationToken.useCachedToken", token))
		property_util.PASSWORD.Set(props, token)
	} else {
		updateErr := f.updateAuthenticationToken(props, region, cacheKey, host, port)
		if updateErr != nil {
			return nil, updateErr
		}
	}

	property_util.USER.Set(props, property_util.DB_USER.Get(props))

	result, err := connectFunc()
	if err != nil {
		updateErr := f.updateAuthenticationToken(props, region, cacheKey, host, port)
		if updateErr != nil {
			return nil, updateErr
		}
		return connectFunc()
	}
	return result, err
}

func (f *FederatedAuthPlugin) updateAuthenticationToken(
	props map[string]string,
	region region_util.Region,
	cacheKey string,
	host string,
	port int) error {
	tokenExpirationSec := property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_TOKEN_EXPIRATION)

	credentialsProvider, err := f.credentialsProviderFactory.GetAwsCredentialsProvider(host, region, props)
	if err != nil {
		return err
	}

	token, err := f.iamTokenUtility.GenerateAuthenticationToken(property_util.DB_USER.Get(props), host, port, region, credentialsProvider)
	if err != nil {
		return err
	}
	slog.Debug(error_util.GetMessage("AuthenticationToken.generatedNewToken", token))
	props[property_util.PASSWORD.Name] = token
	TokenCache.Put(cacheKey, token, time.Second*time.Duration(tokenExpirationSec))
	return nil
}
