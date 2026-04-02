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

package aws_secrets_manager

import (
	"database/sql/driver"
	"log/slog"
	"net/url"
	"time"

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
	awssql.UsePluginFactory(driver_infrastructure.SECRETS_MANAGER_PLUGIN_CODE,
		NewAwsSecretsManagerPluginFactory())
}

var fetchCredentialsCounterName = "secretsManager.fetchCredentials.count"

type AwsSecretsManagerPluginFactory struct{}

func (factory AwsSecretsManagerPluginFactory) GetInstance(servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
) (driver_infrastructure.ConnectionPlugin, error) {
	return NewAwsSecretsManagerPlugin(servicesContainer, props, NewAwsSecretsManagerClient)
}

func (factory AwsSecretsManagerPluginFactory) ClearCaches() {
	SecretsCache.Clear()
}

func NewAwsSecretsManagerPluginFactory() driver_infrastructure.ConnectionPluginFactory {
	return AwsSecretsManagerPluginFactory{}
}

var SecretsCache = utils.NewCache[AwsRdsSecrets]()

type AwsSecretsManagerPlugin struct {
	plugins.BaseConnectionPlugin
	servicesContainer               driver_infrastructure.ServicesContainer
	props                           *utils.RWMap[string, string]
	SecretsCacheKey                 string
	region                          region_util.Region
	endpoint                        string
	awsSecretsManagerClientProvider NewAwsSecretsManagerClientProvider
	secretExpirationTime            time.Duration
	fetchCredentialsCounter         telemetry.TelemetryCounter
	secretUsernameKey               string
	secretPasswordKey               string
}

func NewAwsSecretsManagerPlugin(servicesContainer driver_infrastructure.ServicesContainer,
	props *utils.RWMap[string, string],
	awsSecretsManagerClientProvider NewAwsSecretsManagerClientProvider,
) (*AwsSecretsManagerPlugin, error) {
	pluginService := servicesContainer.GetPluginService()
	// Validate Secret ID
	secretId := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.SECRETS_MANAGER_SECRET_ID)

	if secretId == "" {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("AwsSecretsManagerConnectionPlugin.secretIdMissing", property_util.SECRETS_MANAGER_SECRET_ID.Name))
	}

	secretUsernameKey := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.SECRETS_MANAGER_SECRET_USERNAME_PROPERTY)
	secretPasswordKey := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.SECRETS_MANAGER_SECRET_PASSWORD_PROPERTY)

	if secretUsernameKey == "" {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("AwsSecretsManagerConnectionPlugin.incorrectJsonKey", property_util.SECRETS_MANAGER_SECRET_USERNAME_PROPERTY.Name))
	}

	if secretPasswordKey == "" {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("AwsSecretsManagerConnectionPlugin.incorrectJsonKey", property_util.SECRETS_MANAGER_SECRET_PASSWORD_PROPERTY.Name))
	}

	// Get and validate region
	regionStr, _ := props.Get(property_util.SECRETS_MANAGER_REGION.Name)
	region, err := GetAwsSecretsManagerRegion(regionStr, property_util.SECRETS_MANAGER_SECRET_ID.Get(props))
	if err != nil {
		return nil, err
	}

	// Validate endpoint if supplied
	secretsEndpoint := property_util.SECRETS_MANAGER_ENDPOINT.Get(props)
	if secretsEndpoint != "" {
		_, err := url.ParseRequestURI(secretsEndpoint)
		if err != nil {
			return nil, error_util.NewGenericAwsWrapperError(
				error_util.GetMessage("AwsSecretsManagerConnectionPlugin.endpointOverrideMisconfigured", secretsEndpoint))
		}
	}

	fetchCredentialsCounter, err := pluginService.GetTelemetryFactory().CreateCounter(fetchCredentialsCounterName)
	if err != nil {
		return nil, err
	}
	secretExpirationTime := property_util.GetExpirationValue(props, property_util.SECRETS_MANAGER_EXPIRATION_SEC)

	return &AwsSecretsManagerPlugin{
		servicesContainer: servicesContainer,
		props:             props,
		SecretsCacheKey: getCacheKey(
			property_util.SECRETS_MANAGER_SECRET_ID.Get(props), string(region),
		),
		region:                          region,
		endpoint:                        secretsEndpoint,
		awsSecretsManagerClientProvider: awsSecretsManagerClientProvider,
		secretExpirationTime:            time.Second * time.Duration(secretExpirationTime),
		fetchCredentialsCounter:         fetchCredentialsCounter,
		secretUsernameKey:               secretUsernameKey,
		secretPasswordKey:               secretPasswordKey,
	}, err
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) GetPluginCode() string {
	return driver_infrastructure.SECRETS_MANAGER_PLUGIN_CODE
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD, plugin_helpers.FORCE_CONNECT_METHOD}
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return awsSecretsManagerPlugin.connectInternal(hostInfo, props, connectFunc)
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	_ bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return awsSecretsManagerPlugin.connectInternal(hostInfo, props, connectFunc)
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) connectInternal(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	secret, secretsWasFetched, err := awsSecretsManagerPlugin.updateSecrets(hostInfo, props, false)

	if err != nil {
		slog.Debug(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.failedToFetchCredentials"))
		return nil, err
	}

	// try and connect
	connProps := awsSecretsManagerPlugin.applySecretToProperties(props, secret)
	conn, err := connectFunc(connProps)

	if err == nil {
		if !secretsWasFetched {
			slog.Debug(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.connectedWithCachedSecret"))
		}
		return conn, err
	}

	if awsSecretsManagerPlugin.servicesContainer.GetPluginService().IsLoginError(err) && !secretsWasFetched {
		// Login unsuccessful with cached credentials
		// Try to re-fetch credentials and try again
		secret, secretsWasFetched, err = awsSecretsManagerPlugin.updateSecrets(hostInfo, props, true)

		if secretsWasFetched {
			slog.Debug(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.retryingAfterFetchingNewSecret"))

			connProps = awsSecretsManagerPlugin.applySecretToProperties(props, secret)
			return connectFunc(connProps)
		}
	}

	return nil, err
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) applySecretToProperties(props *utils.RWMap[string, string], secret AwsRdsSecrets) *utils.RWMap[string, string] {
	connProps := utils.NewRWMapFromCopy(props)
	property_util.USER.Set(connProps, secret.Username)
	property_util.PASSWORD.Set(connProps, secret.Password)
	return connProps
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) updateSecrets(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	forceReFetch bool) (AwsRdsSecrets, bool, error) {
	parentCtx := awsSecretsManagerPlugin.servicesContainer.GetPluginService().GetTelemetryContext()
	telemetryFactory := awsSecretsManagerPlugin.servicesContainer.GetPluginService().GetTelemetryFactory()
	telemetryCtx, ctx := telemetryFactory.OpenTelemetryContext(
		telemetry.TELEMETRY_UPDATE_SECRETS, telemetry.NESTED, parentCtx)
	awsSecretsManagerPlugin.servicesContainer.GetPluginService().SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		awsSecretsManagerPlugin.servicesContainer.GetPluginService().SetTelemetryContext(parentCtx)
	}()
	awsSecretsManagerPlugin.fetchCredentialsCounter.Inc(awsSecretsManagerPlugin.servicesContainer.GetPluginService().GetTelemetryContext())

	fetched := false
	var err error

	secret, loaded := SecretsCache.Get(awsSecretsManagerPlugin.SecretsCacheKey)

	if !loaded || forceReFetch {
		secret, err = awsSecretsManagerPlugin.fetchLatestCredentials(hostInfo, props)

		if err == nil {
			fetched = true
			SecretsCache.Put(awsSecretsManagerPlugin.SecretsCacheKey, secret, awsSecretsManagerPlugin.secretExpirationTime)
		} else {
			slog.Error(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"))
			telemetryCtx.SetSuccess(false)
			telemetryCtx.SetError(err)
			return AwsRdsSecrets{}, fetched, err
		}
	} else {
		slog.Debug(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.useCachedSecret"))
	}

	telemetryCtx.SetSuccess(true)
	return secret, fetched, nil
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) fetchLatestCredentials(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string]) (AwsRdsSecrets, error) {
	slog.Debug(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.fetchingLatestCredentials"))

	secret, err := getRdsSecretFromAwsSecretsManager(
		hostInfo,
		props,
		awsSecretsManagerPlugin.endpoint,
		string(awsSecretsManagerPlugin.region),
		awsSecretsManagerPlugin.secretUsernameKey,
		awsSecretsManagerPlugin.secretPasswordKey,
		awsSecretsManagerPlugin.awsSecretsManagerClientProvider)
	return secret, err
}
