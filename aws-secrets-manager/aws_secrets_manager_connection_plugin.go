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

	awssql "github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugin_helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/plugins"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
)

func init() {
	awssql.UsePluginFactory(driver_infrastructure.SECRETS_MANAGER_PLUGIN_CODE,
		NewAwsSecretsManagerPluginFactory())
}

var fetchCredentialsCounterName = "secretsManager.fetchCredentials.count"

// A configured connect retry interval above this raises the cap rather than being clamped by it.
const maxConnectRetryInterval = 30 * time.Second

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
	connectRetryTimeout             time.Duration
	connectRetryInterval            time.Duration
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

	connectRetryTimeoutMs, timeoutErr := property_util.GetPositiveIntProperty(props, property_util.SECRETS_MANAGER_CONNECT_RETRY_TIMEOUT_MS)
	if timeoutErr != nil {
		return nil, timeoutErr
	}
	connectRetryIntervalMs, intervalErr := property_util.GetPositiveIntProperty(props, property_util.SECRETS_MANAGER_CONNECT_RETRY_INTERVAL_MS)
	if intervalErr != nil {
		return nil, intervalErr
	}
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
		connectRetryTimeout:             time.Duration(connectRetryTimeoutMs) * time.Millisecond,
		connectRetryInterval:            time.Duration(connectRetryIntervalMs) * time.Millisecond,
		fetchCredentialsCounter:         fetchCredentialsCounter,
		secretUsernameKey:               secretUsernameKey,
		secretPasswordKey:               secretPasswordKey,
	}, nil
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
	if awsSecretsManagerPlugin.connectRetryTimeout > 0 {
		return awsSecretsManagerPlugin.connectWithRetryBudget(hostInfo, props, connectFunc)
	}
	return awsSecretsManagerPlugin.connectWithSingleRetry(hostInfo, props, connectFunc)
}

// connectWithSingleRetry re-fetches the credentials at most once, and only if the attempt with the cached
// secret failed to log in. Used when secretsManagerConnectRetryTimeoutMs is 0, the default.
func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) connectWithSingleRetry(
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

// connectWithRetryBudget re-fetches the credentials and reconnects with a capped exponential backoff until
// secretsManagerConnectRetryTimeoutMs runs out, to get past a rotation window in which neither the cached
// nor a freshly fetched secret can log in. Blocks the calling goroutine and issues one GetSecretValue per
// retry. See docs/user-guide/using-plugins/UsingTheAwsSecretsManagerPlugin.md.
func (awsSecretsManagerPlugin *AwsSecretsManagerPlugin) connectWithRetryBudget(
	hostInfo *host_info_util.HostInfo,
	props *utils.RWMap[string, string],
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	pluginService := awsSecretsManagerPlugin.servicesContainer.GetPluginService()
	deadline := time.Now().Add(awsSecretsManagerPlugin.connectRetryTimeout)
	// Never sleep longer than the whole budget, and never for a non-positive duration.
	delay := max(time.Millisecond, min(awsSecretsManagerPlugin.connectRetryInterval, awsSecretsManagerPlugin.connectRetryTimeout))
	maxDelay := max(delay, maxConnectRetryInterval)

	// Set once a login failure is seen, and reported if the budget runs out.
	var lastLoginErr error

	for attempt := 1; ; attempt++ {
		// The first attempt may use the cached secret; later attempts force a re-fetch to pick up a
		// version promoted in the meantime.
		secret, _, fetchErr := awsSecretsManagerPlugin.updateSecrets(hostInfo, props, attempt > 1)
		if fetchErr != nil {
			if lastLoginErr == nil {
				slog.Debug(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.failedToFetchCredentials"))
				return nil, fetchErr
			}
			// A transient fetch failure must neither end the loop nor replace the login failure as the
			// reported cause.
			slog.Debug(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.connectRetryFetchFailed", fetchErr))
		} else {
			conn, connErr := connectFunc(awsSecretsManagerPlugin.applySecretToProperties(props, secret))
			if connErr == nil {
				if attempt > 1 {
					slog.Debug(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.connectRetrySucceeded", attempt))
				}
				return conn, nil
			}
			// A driver may hand back a connection alongside an error. Do not leak one per attempt.
			if conn != nil {
				_ = conn.Close()
			}
			if !pluginService.IsLoginError(connErr) {
				// Not a credentials problem, so re-fetching and retrying would not help.
				return nil, connErr
			}
			lastLoginErr = connErr
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			slog.Warn(error_util.GetMessage("AwsSecretsManagerConnectionPlugin.connectRetryBudgetExhausted",
				attempt, awsSecretsManagerPlugin.connectRetryTimeout))
			return nil, lastLoginErr
		}
		time.Sleep(min(delay, remaining))
		delay = min(delay*2, maxDelay)
	}
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
