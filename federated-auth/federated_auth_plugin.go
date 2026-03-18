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
	"github.com/aws/aws-sdk-go-v2/aws"
)

func init() {
	awssql.UsePluginFactory(driver_infrastructure.ADFS_PLUGIN_CODE,
		NewFederatedAuthPluginFactory())
}

var TokenCache = utils.NewCache[string]()

type FederatedAuthPluginFactory struct{}

func (f FederatedAuthPluginFactory) GetInstance(
	servicesContainer driver_infrastructure.ServicesContainer,
	_ *utils.RWMap[string, string]) (driver_infrastructure.ConnectionPlugin, error) {
	providerFactory := NewAdfsCredentialsProviderFactory(auth_helpers.GetBasicHttpClient, auth_helpers.NewAwsStsClient, servicesContainer.GetPluginService())
	return NewFederatedAuthPlugin(servicesContainer, providerFactory, &auth_helpers.RegularIamTokenUtility{})
}

func (f FederatedAuthPluginFactory) ClearCaches() {
	TokenCache.Clear()
}

func NewFederatedAuthPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return FederatedAuthPluginFactory{}
}

type FederatedAuthPlugin struct {
	servicesContainer          driver_infrastructure.ServicesContainer
	credentialsProviderFactory auth_helpers.CredentialsProviderFactory
	iamTokenUtility            auth_helpers.IamTokenUtility
	fetchTokenCounter          telemetry.TelemetryCounter
	plugins.BaseConnectionPlugin
}

func NewFederatedAuthPlugin(
	servicesContainer driver_infrastructure.ServicesContainer,
	providerFactory auth_helpers.CredentialsProviderFactory,
	iamTokenUtility auth_helpers.IamTokenUtility) (*FederatedAuthPlugin, error) {
	pluginService := servicesContainer.GetPluginService()
	fetchTokenCounter, err := pluginService.GetTelemetryFactory().CreateCounter("federatedAuth.fetchToken.count")
	if err != nil {
		return nil, err
	}
	return &FederatedAuthPlugin{
		servicesContainer:          servicesContainer,
		credentialsProviderFactory: providerFactory,
		iamTokenUtility:            iamTokenUtility,
		fetchTokenCounter:          fetchTokenCounter,
	}, nil
}

func (f *FederatedAuthPlugin) GetPluginCode() string {
	return driver_infrastructure.ADFS_PLUGIN_CODE
}

func (f *FederatedAuthPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD, plugin_helpers.FORCE_CONNECT_METHOD}
}

func (f *FederatedAuthPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return f.connectInternal(hostInfo, props, connectFunc)
}

func (f *FederatedAuthPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return f.connectInternal(hostInfo, props, connectFunc)
}

func (f *FederatedAuthPlugin) connectInternal(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	property_util.CheckIdpCredentialsWithFallback(property_util.IDP_USERNAME, property_util.IDP_PASSWORD, props)

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
		f.servicesContainer.GetPluginService().GetDialect().GetDefaultPort())

	var credentialsProvider aws.CredentialsProvider
	var regionProvider region_util.RegionProvider
	if utils.IsGlobalDbWriterClusterDns(host) {
		credentialsProvider, err = f.credentialsProviderFactory.GetAwsCredentialsProvider(host, "", props)
		if err != nil {
			return nil, err
		}
		regionProvider = auth_helpers.NewGdbRegionProvider(credentialsProvider)
	} else {
		regionProvider = &region_util.DefaultRegionProvider{}
	}
	region, regionErr := regionProvider.GetRegion(host, props, property_util.IAM_REGION)
	if regionErr != nil {
		return nil, regionErr
	}
	if region == "" {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("FederatedAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name))
	}

	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.DB_USER),
		host,
		port,
		region)

	token, isCachedToken := TokenCache.Get(cacheKey)

	if isCachedToken {
		slog.Debug(error_util.GetMessage("AuthenticationToken.useCachedToken"))
		property_util.PASSWORD.Set(props, token)
	} else {
		updateErr := f.updateAuthenticationToken(props, region, cacheKey, host, port)
		if updateErr != nil {
			return nil, updateErr
		}
	}

	property_util.USER.Set(props, property_util.DB_USER.Get(props))

	result, err := connectFunc(props)
	if err != nil && f.servicesContainer.GetPluginService().IsLoginError(err) && isCachedToken {
		updateErr := f.updateAuthenticationToken(props, region, cacheKey, host, port)
		if updateErr != nil {
			return nil, updateErr
		}
		return connectFunc(props)
	}
	return result, err
}

func (f *FederatedAuthPlugin) updateAuthenticationToken(
	props *utils.RWMap[string, string],
	region region_util.Region,
	cacheKey string,
	host string,
	port int) error {
	tokenExpirationSec := property_util.GetExpirationValue(props, property_util.IAM_TOKEN_EXPIRATION_SEC)
	credentialsProvider, err := f.credentialsProviderFactory.GetAwsCredentialsProvider(host, region, props)
	if err != nil {
		return err
	}

	f.fetchTokenCounter.Inc(f.servicesContainer.GetPluginService().GetTelemetryContext())
	token, err := f.iamTokenUtility.GenerateAuthenticationToken(property_util.DB_USER.Get(props), host, port, region, credentialsProvider, f.servicesContainer.GetPluginService())
	if err != nil {
		return err
	}
	slog.Debug(error_util.GetMessage("AuthenticationToken.generatedNewToken"))
	property_util.PASSWORD.Set(props, token)
	TokenCache.Put(cacheKey, token, time.Second*time.Duration(tokenExpirationSec))
	return nil
}
