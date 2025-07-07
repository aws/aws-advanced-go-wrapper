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

package iam

import (
	"database/sql/driver"
	"errors"
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
	awssql.UsePluginFactory("iam", NewIamAuthPluginFactory())
}

type IamAuthPluginFactory struct{}

func (factory IamAuthPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService, props map[string]string) (driver_infrastructure.ConnectionPlugin, error) {
	return NewIamAuthPlugin(pluginService, auth_helpers.RegularIamTokenUtility{}, props)
}

func (factory IamAuthPluginFactory) ClearCaches() {
	TokenCache.Clear()
}

func NewIamAuthPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return IamAuthPluginFactory{}
}

var TokenCache = utils.NewCache[string]()

type IamAuthPlugin struct {
	plugins.BaseConnectionPlugin
	pluginService     driver_infrastructure.PluginService
	iamTokenUtility   auth_helpers.IamTokenUtility
	props             map[string]string
	fetchTokenCounter telemetry.TelemetryCounter
}

func NewIamAuthPlugin(pluginService driver_infrastructure.PluginService, iamTokenUtility auth_helpers.IamTokenUtility, props map[string]string) (*IamAuthPlugin, error) {
	fetchTokenCounter, err := pluginService.GetTelemetryFactory().CreateCounter("iam.fetchToken.count")
	if err != nil {
		return nil, err
	}

	return &IamAuthPlugin{
		pluginService:     pluginService,
		props:             props,
		iamTokenUtility:   iamTokenUtility,
		fetchTokenCounter: fetchTokenCounter,
	}, nil
}

func (iamAuthPlugin *IamAuthPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD, plugin_helpers.FORCE_CONNECT_METHOD}
}

func (iamAuthPlugin *IamAuthPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return iamAuthPlugin.connectInternal(hostInfo, props, connectFunc)
}

func (iamAuthPlugin *IamAuthPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return iamAuthPlugin.connectInternal(hostInfo, props, connectFunc)
}

func (iamAuthPlugin *IamAuthPlugin) connectInternal(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	host := auth_helpers.GetIamHost(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.IAM_HOST), *hostInfo)

	port := auth_helpers.GetIamPort(
		property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.IAM_DEFAULT_PORT),
		*hostInfo, iamAuthPlugin.pluginService.GetDialect().GetDefaultPort())

	region := region_util.GetRegion(host, props, property_util.IAM_REGION)
	if region == "" {
		return nil, errors.New(error_util.GetMessage("IamAuthPlugin.unableToDetermineRegion", property_util.IAM_REGION.Name))
	}

	cacheKey := auth_helpers.GetCacheKey(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.USER),
		host,
		port,
		region,
	)

	propsCopy := utils.CreateMapCopy(props)
	token, cachedTokenFound := TokenCache.Get(cacheKey)
	isCachedToken := cachedTokenFound && token != ""
	if isCachedToken {
		slog.Debug(error_util.GetMessage("IamAuthPlugin.useCachedToken"))
		property_util.PASSWORD.Set(propsCopy, token)
	} else {
		err := iamAuthPlugin.fetchAndSetToken(hostInfo, host, port, region, cacheKey, propsCopy)
		if err != nil {
			return nil, err
		}
	}

	conn, err := connectFunc(propsCopy)
	if err == nil {
		return conn, nil
	} else {
		slog.Debug(error_util.GetMessage("IamAuthPlugin.connectionError", err))
		if !iamAuthPlugin.pluginService.IsLoginError(err) || !isCachedToken {
			return nil, err
		}
	}

	// Login unsuccessful with cached token
	// Try to generate a new token and connect again
	err = iamAuthPlugin.fetchAndSetToken(hostInfo, host, port, region, cacheKey, propsCopy)
	if err != nil {
		return nil, err
	}

	return connectFunc(propsCopy)
}

func (iamAuthPlugin *IamAuthPlugin) fetchAndSetToken(
	hostInfo *host_info_util.HostInfo,
	host string,
	port int,
	region region_util.Region,
	cacheKey string,
	props map[string]string) error {
	tokenExpirationSec := property_util.GetExpirationValue(props, property_util.IAM_EXPIRATION_SEC)
	awsCredentialsProvider, err := auth_helpers.GetAwsCredentialsProvider(*hostInfo, props)
	if err != nil {
		slog.Error(error_util.GetMessage("IamAuthPlugin.errorGettingAwsCredentialsProvider", err))
		return err
	}
	iamAuthPlugin.fetchTokenCounter.Inc(iamAuthPlugin.pluginService.GetTelemetryContext())
	token, err := iamAuthPlugin.iamTokenUtility.GenerateAuthenticationToken(
		property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.USER),
		host,
		port,
		region,
		awsCredentialsProvider,
		iamAuthPlugin.pluginService)
	if err != nil || token == "" {
		slog.Debug(error_util.GetMessage("IamAuthPlugin.errorGeneratingNewToken", err))
		return err
	}
	slog.Debug(error_util.GetMessage("AuthenticationToken.generatedNewToken"))
	property_util.PASSWORD.Set(props, token)
	TokenCache.Put(cacheKey, token, time.Duration(tokenExpirationSec)*time.Second)
	return nil
}
