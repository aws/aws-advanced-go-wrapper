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
	awssql.UsePluginFactory("awsSecretsManager", NewAwsSecretsManagerPluginFactory())
}

var fetchCredentialsCounterName = "secretsManager.fetchCredentials.count"

type AwsSecretsManagerPluginFactory struct{}

func (factory AwsSecretsManagerPluginFactory) GetInstance(pluginService driver_infrastructure.PluginService,
	props map[string]string,
) (driver_infrastructure.ConnectionPlugin, error) {
	return NewAwsSecretsManagerPlugin(pluginService, props, NewAwsSecretsManagerClient)
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
	pluginService                   driver_infrastructure.PluginService
	props                           map[string]string
	secret                          AwsRdsSecrets
	SecretsCacheKey                 string
	region                          region_util.Region
	endpoint                        string
	awsSecretsManagerClientProvider NewAwsSecretsManagerClientProvider
	secretExpirationTimeSec         time.Duration
	fetchCredentialsCounter         telemetry.TelemetryCounter
}

func NewAwsSecretsManagerPlugin(pluginService driver_infrastructure.PluginService,
	props map[string]string,
	awsSecretsManagerClientProvider NewAwsSecretsManagerClientProvider,
) (*AwsSecretsManagerPlugin, error) {
	// Validate Secret ID
	secretId := property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.SECRETS_MANAGER_SECRET_ID)

	if secretId == "" {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("AwsSecretsManagerConnectionPlugin.secretIdMissing", property_util.SECRETS_MANAGER_SECRET_ID.Name))
	}

	// Get and validate region
	region, err := GetAwsSecretsManagerRegion(props[property_util.SECRETS_MANAGER_REGION.Name], props[property_util.SECRETS_MANAGER_SECRET_ID.Name])
	if err != nil {
		return nil, err
	}

	// Validate endpoint if supplied
	secretsEndpoint := props[property_util.SECRETS_MANAGER_ENDPOINT.Name]

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

	return &AwsSecretsManagerPlugin{
		pluginService: pluginService,
		props:         props,
		SecretsCacheKey: getCacheKey(
			props[property_util.SECRETS_MANAGER_SECRET_ID.Name], string(region),
		),
		region:                          region,
		endpoint:                        secretsEndpoint,
		awsSecretsManagerClientProvider: awsSecretsManagerClientProvider,
		secretExpirationTimeSec:         time.Second * time.Duration(property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.SECRETS_MANAGER_EXPIRATION_SEC)),
		fetchCredentialsCounter:         fetchCredentialsCounter,
	}, err
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) GetSubscribedMethods() []string {
	return []string{plugin_helpers.CONNECT_METHOD, plugin_helpers.FORCE_CONNECT_METHOD}
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) Connect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return awsSecretsManagerPlugin.connectInternal(hostInfo, props, connectFunc)
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) ForceConnect(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	isInitialConnection bool,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	return awsSecretsManagerPlugin.connectInternal(hostInfo, props, connectFunc)
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) connectInternal(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	secretsWasFetched, _ := awsSecretsManagerPlugin.updateSecrets(hostInfo, props, false)

	// try and connect
	awsSecretsManagerPlugin.applySecretToProperties(props)
	conn, err := connectFunc()

	if err == nil {
		if !secretsWasFetched {
			slog.Debug("AwsSecretsManagerConnectionPlugin: Connected successfully using cached secret.")
		}
		return conn, err
	}

	if awsSecretsManagerPlugin.pluginService.IsLoginError(err) && !secretsWasFetched {
		// Login unsuccessful with cached credentials
		// Try to re-fetch credentials and try again
		secretsWasFetched, err = awsSecretsManagerPlugin.updateSecrets(hostInfo, props, true)

		if secretsWasFetched {
			slog.Debug("AwsSecretsManagerConnectionPlugin: failed initial connection, trying again after fetching new secret value.")

			awsSecretsManagerPlugin.applySecretToProperties(props)
			return connectFunc()
		}
	}

	return nil, err
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) applySecretToProperties(
	props map[string]string) {
	props[property_util.USER.Name] = awsSecretsManagerPlugin.secret.Username
	props[property_util.PASSWORD.Name] = awsSecretsManagerPlugin.secret.Password
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) updateSecrets(
	hostInfo *host_info_util.HostInfo,
	props map[string]string,
	forceReFetch bool) (bool, error) {
	parentCtx := awsSecretsManagerPlugin.pluginService.GetTelemetryContext()
	telemetryCtx, ctx := awsSecretsManagerPlugin.pluginService.GetTelemetryFactory().OpenTelemetryContext(telemetry.TELEMETRY_UPDATE_SECRETS, telemetry.NESTED, parentCtx)
	awsSecretsManagerPlugin.pluginService.SetTelemetryContext(ctx)
	defer func() {
		telemetryCtx.CloseContext()
		awsSecretsManagerPlugin.pluginService.SetTelemetryContext(parentCtx)
	}()
	awsSecretsManagerPlugin.fetchCredentialsCounter.Inc(awsSecretsManagerPlugin.pluginService.GetTelemetryContext())

	fetched := false
	var err error

	secret, loaded := SecretsCache.Get(awsSecretsManagerPlugin.SecretsCacheKey)

	if !loaded || forceReFetch {
		secret, err = awsSecretsManagerPlugin.fetchLatestCredentials(hostInfo, props)

		if err == nil {
			fetched = true
			SecretsCache.Put(awsSecretsManagerPlugin.SecretsCacheKey, secret, awsSecretsManagerPlugin.secretExpirationTimeSec)
		} else {
			slog.Error(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.failedToFetchDbCredentials"))
			telemetryCtx.SetSuccess(false)
			telemetryCtx.SetError(err)
			return fetched, err
		}
	}

	telemetryCtx.SetSuccess(true)
	awsSecretsManagerPlugin.secret = secret
	return fetched, nil
}

func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) fetchLatestCredentials(
	hostInfo *host_info_util.HostInfo,
	props map[string]string) (AwsRdsSecrets, error) {
	slog.Debug("AwsSecretsManagerConnectionPlugin: Fetching latest credentials")

	secret, err := getRdsSecretFromAwsSecretsManager(
		hostInfo,
		props,
		awsSecretsManagerPlugin.endpoint,
		string(awsSecretsManagerPlugin.region),
		awsSecretsManagerPlugin.awsSecretsManagerClientProvider)
	return secret, err
}
