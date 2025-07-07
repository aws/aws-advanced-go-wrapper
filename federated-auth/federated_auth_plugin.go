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
	"database/sql/driver"
	"log/slog"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
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
)

func init() {
	awssql.UsePluginFactory("federatedAuth", NewFederatedAuthPluginFactory())
}

var TokenCache = utils.NewCache[string]()

type FederatedAuthPluginFactory struct{}

func (f FederatedAuthPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService, props map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	providerFactory := NewAdfsCredentialsProviderFactory(auth_helpers.GetBasicHttpClient, auth_helpers.NewAwsStsClient, pluginService)
	return NewFederatedAuthPlugin(pluginService, providerFactory, auth_helpers.RegularIamTokenUtility{})
}

func (f FederatedAuthPluginFactory) ClearCaches() {
	TokenCache.Clear()
}

func NewFederatedAuthPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return FederatedAuthPluginFactory{}
}

type FederatedAuthPlugin struct {
	pluginService              driver_infrastructure.PluginService
	credentialsProviderFactory auth_helpers.CredentialsProviderFactory
	iamTokenUtility            auth_helpers.IamTokenUtility
	fetchTokenCounter          telemetry.TelemetryCounter
	plugins.BaseConnectionPlugin
}

func NewFederatedAuthPlugin(
	pluginService driver_infrastructure.PluginService,
	providerFactory auth_helpers.CredentialsProviderFactory,
	iamTokenUtility auth_helpers.IamTokenUtility) (*FederatedAuthPlugin, error) {
	fetchTokenCounter, err := pluginService.GetTelemetryFactory().CreateCounter("federatedAuth.fetchToken.count")
	if err != nil {
		return nil, err
	}
	return &FederatedAuthPlugin{
		pluginService:              pluginService,
		credentialsProviderFactory: providerFactory,
		iamTokenUtility:            iamTokenUtility,
		fetchTokenCounter:          fetchTokenCounter,
	}, nil
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

	err := auth_helpers.ValidateAuthParams("adfs",
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_USERNAME),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_PASSWORD),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IDP_ENDPOINT),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_ROLE_ARN),
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_IDP_ARN),
		"",
	)
	if err != nil {
		return nil, err
	}

	host := auth_helpers.GetIamHost(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_HOST), *hostInfo)

	port := auth_helpers.GetIamPort(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_DEFAULT_PORT),
		*hostInfo,
		f.pluginService.GetDialect().GetDefaultPort())

	region := region_util.GetRegion(host, props, property_util.IAM_REGION)
	if region == "" {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("FederatedAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name))
	}

	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		region)

	token, isCachedToken := TokenCache.Get(cacheKey)
	propsCopy := utils.CreateMapCopy(props)

	if isCachedToken {
		slog.Debug(error_util.GetMessage("AuthenticationToken.useCachedToken"))
		property_util.PASSWORD.Set(propsCopy, token)
	} else {
		updateErr := f.updateAuthenticationToken(propsCopy, region, cacheKey, host, port)
		if updateErr != nil {
			return nil, updateErr
		}
	}

	property_util.USER.Set(propsCopy, property_util.DB_USER.Get(propsCopy))

	result, err := connectFunc(propsCopy)
	if err != nil && f.pluginService.IsLoginError(err) && isCachedToken {
		updateErr := f.updateAuthenticationToken(propsCopy, region, cacheKey, host, port)
		if updateErr != nil {
			return nil, updateErr
		}
		return connectFunc(propsCopy)
	}
	return result, err
}

func (f *FederatedAuthPlugin) updateAuthenticationToken(
	props map[string]string,
	region region_util.Region,
	cacheKey string,
	host string,
	port int) error {
	tokenExpirationSec := property_util.GetExpirationValue(props, property_util.IAM_TOKEN_EXPIRATION_SEC)
	credentialsProvider, err := f.credentialsProviderFactory.GetAwsCredentialsProvider(host, region, props)
	if err != nil {
		return err
	}

	f.fetchTokenCounter.Inc(f.pluginService.GetTelemetryContext())
	token, err := f.iamTokenUtility.GenerateAuthenticationToken(property_util.DB_USER.Get(props), host, port, region, credentialsProvider, f.pluginService)
	if err != nil {
		return err
	}
	slog.Debug(error_util.GetMessage("AuthenticationToken.generatedNewToken"))
	property_util.PASSWORD.Set(props, token)
	TokenCache.Put(cacheKey, token, time.Second*time.Duration(tokenExpirationSec))
	return nil
}
