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

package property_util

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
)

const DEFAULT_PLUGINS = "failover,efm"
const MONITORING_PROPERTY_PREFIX = "monitoring-"
const LIMITLESS_PROPERTY_PREFIX = "limitless"
const INTERNAL_CONNECT_PROPERTY_NAME = "76c06979-49c4-4c86-9600-a63605b83f50"
const SET_READ_ONLY_CTX_KEY = "setReadOnly"
const BG_PROPERTY_PREFIX = "blue-green-monitoring-"

var PREFIXES = []string{
	MONITORING_PROPERTY_PREFIX,
	INTERNAL_CONNECT_PROPERTY_NAME,
	LIMITLESS_PROPERTY_PREFIX,
	BG_PROPERTY_PREFIX,
}

type WrapperPropertyType int

const (
	WRAPPER_TYPE_INT    WrapperPropertyType = 1
	WRAPPER_TYPE_STRING WrapperPropertyType = 2
	WRAPPER_TYPE_BOOL   WrapperPropertyType = 3
)

type AwsWrapperProperty struct {
	Name                string
	description         string
	defaultValue        string
	wrapperPropertyType WrapperPropertyType
}

func (prop *AwsWrapperProperty) Get(props map[string]string) string {
	var result, ok = props[prop.Name]
	if !ok {
		return prop.defaultValue
	}
	return result
}

func (prop *AwsWrapperProperty) Set(props map[string]string, val string) {
	props[prop.Name] = val
}

func GetVerifiedWrapperPropertyValue[T any](props map[string]string, property AwsWrapperProperty) T {
	propValue := property.Get(props)
	var parsedValue any
	var err error
	switch property.wrapperPropertyType {
	case WRAPPER_TYPE_INT:
		parsedValue, err = strconv.Atoi(propValue)
		if err != nil {
			slog.Warn(fmt.Sprintf("Using default value '%s' for property '%s' after encountering an error: '%s'.", property.defaultValue, property.Name, err.Error()))
			parsedValue, _ = strconv.Atoi(property.defaultValue)
		}
	case WRAPPER_TYPE_BOOL:
		parsedValue, err = strconv.ParseBool(propValue)
		if err != nil {
			slog.Warn(fmt.Sprintf("Using default value '%s' for property '%s' after encountering an error: '%s'.", property.defaultValue, property.Name, err.Error()))
			parsedValue, _ = strconv.ParseBool(property.defaultValue)
		}
	default: // Default type is: WRAPPER_TYPE_STRING.
		parsedValue = propValue
	}

	result, ok := parsedValue.(T)
	if !ok {
		// This should never be called.
		slog.Warn(error_util.GetMessage("AwsWrapperProperty.unexpectedType", property.Name, propValue))
	}

	return result
}

func GetPositiveIntProperty(props map[string]string, property AwsWrapperProperty) (int, error) {
	val := GetVerifiedWrapperPropertyValue[int](props, property)
	if val < 0 {
		return 0, error_util.NewGenericAwsWrapperError(error_util.GetMessage("AwsWrapperProperty.requiresNonNegativeIntValue", property.Name))
	}
	return val, nil
}

func GetHttpTimeoutValue(props map[string]string) int {
	val := GetVerifiedWrapperPropertyValue[int](props, HTTP_TIMEOUT_MS)
	if val <= 0 {
		slog.Error(error_util.GetMessage("AwsWrapperProperty.noTimeoutValue", HTTP_TIMEOUT_MS.Name, val))
	}
	return val
}

func GetExpirationValue(props map[string]string, property AwsWrapperProperty) int {
	val := GetVerifiedWrapperPropertyValue[int](props, property)
	if val <= 0 {
		slog.Error(error_util.GetMessage("AwsWrapperProperty.noExpirationValue", property.Name, val))
	}
	return val
}

func GetRefreshRateValue(props map[string]string, property AwsWrapperProperty) int {
	val := GetVerifiedWrapperPropertyValue[int](props, property)
	if val <= 0 {
		slog.Error(error_util.GetMessage("AwsWrapperProperty.noRefreshRateValue", property.Name, val))
	}
	return val
}

var ALL_WRAPPER_PROPERTIES = map[string]bool{
	USER.Name:                                      true,
	PASSWORD.Name:                                  true,
	HOST.Name:                                      true,
	PORT.Name:                                      true,
	DATABASE.Name:                                  true,
	DRIVER_PROTOCOL.Name:                           true,
	NET.Name:                                       true,
	SINGLE_WRITER_DSN.Name:                         true,
	PLUGINS.Name:                                   true,
	AUTO_SORT_PLUGIN_ORDER.Name:                    true,
	DIALECT.Name:                                   true,
	TARGET_DRIVER_DIALECT.Name:                     true,
	TARGET_DRIVER_AUTO_REGISTER.Name:               true,
	CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Name:          true,
	CLUSTER_ID.Name:                                true,
	CLUSTER_INSTANCE_HOST_PATTERN.Name:             true,
	AWS_PROFILE.Name:                               true,
	IAM_HOST.Name:                                  true,
	IAM_EXPIRATION_SEC.Name:                        true,
	IAM_REGION.Name:                                true,
	IAM_DEFAULT_PORT.Name:                          true,
	SECRETS_MANAGER_SECRET_ID.Name:                 true,
	SECRETS_MANAGER_REGION.Name:                    true,
	SECRETS_MANAGER_ENDPOINT.Name:                  true,
	SECRETS_MANAGER_EXPIRATION_SEC.Name:            true,
	FAILURE_DETECTION_TIME_MS.Name:                 true,
	FAILURE_DETECTION_INTERVAL_MS.Name:             true,
	FAILURE_DETECTION_COUNT.Name:                   true,
	MONITOR_DISPOSAL_TIME_MS.Name:                  true,
	FAILOVER_TIMEOUT_MS.Name:                       true,
	FAILOVER_MODE.Name:                             true,
	FAILOVER_READER_HOST_SELECTOR_STRATEGY.Name:    true,
	ENABLE_CONNECT_FAILOVER.Name:                   true,
	CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.Name:     true,
	WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.Name:         true,
	IAM_TOKEN_EXPIRATION_SEC.Name:                  true,
	IDP_USERNAME.Name:                              true,
	IDP_PASSWORD.Name:                              true,
	IDP_PORT.Name:                                  true,
	IAM_ROLE_ARN.Name:                              true,
	IAM_IDP_ARN.Name:                               true,
	IDP_ENDPOINT.Name:                              true,
	RELAYING_PARTY_ID.Name:                         true,
	DB_USER.Name:                                   true,
	APP_ID.Name:                                    true,
	HTTP_TIMEOUT_MS.Name:                           true,
	SSL_INSECURE.Name:                              true,
	ENABLE_TELEMETRY.Name:                          true,
	TELEMETRY_SUBMIT_TOP_LEVEL.Name:                true,
	TELEMETRY_TRACES_BACKEND.Name:                  true,
	TELEMETRY_METRICS_BACKEND.Name:                 true,
	TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE.Name:   true,
	LIMITLESS_MONITORING_INTERVAL_MS.Name:          true,
	LIMITLESS_MONITORING_DISPOSAL_TIME_MS.Name:     true,
	LIMITLESS_ROUTER_CACHE_EXPIRATION_TIME_MS.Name: true,
	LIMITLESS_WAIT_FOR_ROUTER_INFO.Name:            true,
	LIMITLESS_GET_ROUTER_MAX_RETRIES.Name:          true,
	LIMITLESS_GET_ROUTER_RETRY_INTERVAL_MS.Name:    true,
	LIMITLESS_MAX_CONN_RETRIES.Name:                true,
	LIMITLESS_ROUTER_QUERY_TIMEOUT_MS.Name:         true,
	TRANSFER_SESSION_STATE_ON_SWITCH.Name:          true,
	RESET_SESSION_STATE_ON_CLOSE.Name:              true,
	ROLLBACK_ON_SWITCH.Name:                        true,
	READER_HOST_SELECTOR_STRATEGY.Name:             true,
	BG_CONNECT_TIMEOUT_MS.Name:                     true,
	BGD_ID.Name:                                    true,
	BG_INTERVAL_BASELINE_MS.Name:                   true,
	BG_INTERVAL_INCREASED_MS.Name:                  true,
	BG_INTERVAL_HIGH_MS.Name:                       true,
	BG_SWITCHOVER_TIMEOUT_MS.Name:                  true,
	BG_SUSPEND_NEW_BLUE_CONNECTIONS.Name:           true,
}

var USER = AwsWrapperProperty{
	Name:                "user",
	description:         "The user name that the driver will use to connect to database.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var PASSWORD = AwsWrapperProperty{
	Name:                "password",
	description:         "The password that the driver will use to connect to database.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var HOST = AwsWrapperProperty{
	Name:                "host",
	description:         "The host name of the database server the driver wil connect to.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var PORT = AwsWrapperProperty{
	Name:                "port",
	description:         "The port of the database server the driver will connection to.",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var DATABASE = AwsWrapperProperty{
	Name:                "database",
	description:         "The name of the database the driver will connect to.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var DRIVER_PROTOCOL = AwsWrapperProperty{
	Name:                "protocol",
	description:         "The underlying driver protocol AWS Go driver will connect using.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var NET = AwsWrapperProperty{
	Name:                "net",
	description:         "The named network to connect with.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var SINGLE_WRITER_DSN = AwsWrapperProperty{
	Name: "singleWriterDsn",
	description: "Set to true if you are providing a dsn with multiple comma-delimited hosts and your cluster has only one writer. " +
		"The writer must be the first host in the dsn.",
	defaultValue:        "false",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var PLUGINS = AwsWrapperProperty{
	Name:                "plugins",
	description:         "Comma separated list of connection plugin codes.",
	defaultValue:        DEFAULT_PLUGINS,
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var AUTO_SORT_PLUGIN_ORDER = AwsWrapperProperty{
	Name: "autoSortPluginOrder",
	description: "This flag is enabled by default, meaning that the plugins order will be automatically adjusted. Disable it at your own " +
		"risk or if you really need plugins to be executed in a particular order.",
	defaultValue:        "true",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var DIALECT = AwsWrapperProperty{
	Name:                "databaseDialect",
	description:         "A unique identifier for the supported database dialect.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var TARGET_DRIVER_DIALECT = AwsWrapperProperty{
	Name:                "targetDriverDialect",
	description:         "A unique identifier for the target driver dialect.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var TARGET_DRIVER_AUTO_REGISTER = AwsWrapperProperty{
	Name:                "targetDriverAutoRegister",
	description:         "Allows the AWS Advanced Go Wrapper to auto-register a target driver.",
	defaultValue:        "true",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var CLUSTER_TOPOLOGY_REFRESH_RATE_MS = AwsWrapperProperty{
	Name: "clusterTopologyRefreshRateMs",
	description: "Cluster topology refresh rate in millis. " +
		"The cached topology for the cluster will be invalidated after the specified time, " +
		"after which it will be updated during the next interaction with the connection.",
	defaultValue:        "30000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var CLUSTER_ID = AwsWrapperProperty{
	Name: "clusterId",
	description: "A unique identifier for the cluster. " +
		"Connections with the same cluster id share a cluster topology cache. " +
		"If unspecified, a cluster id is automatically created for AWS RDS clusters.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var CLUSTER_INSTANCE_HOST_PATTERN = AwsWrapperProperty{
	Name: "clusterInstanceHostPattern",
	description: "The cluster instance DNS pattern that will be used to build a complete instance endpoint. " +
		"A \"?\" character in this pattern should be used as a placeholder for cluster instance names. " +
		"This pattern is required to be specified for IP address or custom domain connections to AWS RDS " +
		"clusters. Otherwise, if unspecified, the pattern will be automatically created for AWS RDS clusters.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var FAILURE_DETECTION_TIME_MS = AwsWrapperProperty{
	Name:                "failureDetectionTimeMs",
	description:         "Interval in millis between sending SQL to the server and the first probe to database host.",
	defaultValue:        "30000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var FAILURE_DETECTION_INTERVAL_MS = AwsWrapperProperty{
	Name:                "failureDetectionIntervalMs",
	description:         "Interval in millis between probes to database host.",
	defaultValue:        "5000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var FAILURE_DETECTION_COUNT = AwsWrapperProperty{
	Name:                "failureDetectionCount",
	description:         "Number of failed connection checks before considering database host unhealthy.",
	defaultValue:        "3",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var MONITOR_DISPOSAL_TIME_MS = AwsWrapperProperty{
	Name:                "monitorDisposalTimeMs",
	description:         "Interval in milliseconds for a monitor to be considered inactive and to be disposed.",
	defaultValue:        "600000", // 10 minutes.
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var AWS_PROFILE = AwsWrapperProperty{
	Name:                "awsProfile",
	description:         "Name of the AWS Profile to use for IAM or SecretsManager auth.",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IAM_HOST = AwsWrapperProperty{
	Name:                "iamHost",
	description:         "Overrides the host that is used to generate the IAM token.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IAM_DEFAULT_PORT = AwsWrapperProperty{
	Name:                "iamDefaultPort",
	description:         "Overrides default port that is used to generate the IAM token.",
	defaultValue:        "-1",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var IAM_REGION = AwsWrapperProperty{
	Name:                "iamRegion",
	description:         "Overrides AWS region that is used to generate the IAM token.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IAM_EXPIRATION_SEC = AwsWrapperProperty{
	Name:                "iamExpirationSec",
	description:         "IAM token cache expiration in seconds.",
	defaultValue:        "870",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var FAILOVER_TIMEOUT_MS = AwsWrapperProperty{
	Name:                "failoverTimeoutMs",
	description:         "Maximum allowed time for the failover process.",
	defaultValue:        "300000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var FAILOVER_MODE = AwsWrapperProperty{
	Name:                "failoverMode",
	description:         "Set host role to follow during failover.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var FAILOVER_READER_HOST_SELECTOR_STRATEGY = AwsWrapperProperty{
	Name:                "failoverReaderHostSelectorStrategy",
	description:         "The strategy that should be used to select a new reader host when opening a new connection during failover.",
	defaultValue:        "random",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var ENABLE_CONNECT_FAILOVER = AwsWrapperProperty{
	Name: "enableConnectFailover",
	description: "Enable/disable cluster-aware failover if the initial connection to the database fails due to a " +
		"network error. Note that this may result in a connection to a different instance in the cluster " +
		"than was specified by the DSN.",
	defaultValue:        "true",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS = AwsWrapperProperty{
	Name:                "clusterTopologyHighRefreshRateMs",
	description:         "Cluster topology high refresh rate in milliseconds.",
	defaultValue:        "100",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var SECRETS_MANAGER_SECRET_ID = AwsWrapperProperty{
	Name:                "secretsManagerSecretId",
	description:         "The name or the ARN of the secret to retrieve.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var SECRETS_MANAGER_REGION = AwsWrapperProperty{
	Name:                "secretsManagerRegion",
	description:         "The region of the secret to retrieve.",
	defaultValue:        "us-east-1",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var SECRETS_MANAGER_ENDPOINT = AwsWrapperProperty{
	Name:                "secretsManagerEndpoint",
	description:         "The endpoint of the secret to retrieve.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var SECRETS_MANAGER_EXPIRATION_SEC = AwsWrapperProperty{
	Name:                "secretsManagerExpirationSec",
	description:         "The time in seconds that secrets are cached for.",
	defaultValue:        "870",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS = AwsWrapperProperty{
	Name:                "weightedRandomHostWeightPairs",
	description:         "Comma separated list of database host-weight pairs in the format of `<host>:<weight>`.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IAM_TOKEN_EXPIRATION_SEC = AwsWrapperProperty{
	Name:                "iamTokenExpirationSec",
	description:         "IAM token cache expiration in seconds.",
	defaultValue:        "870",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var IDP_USERNAME = AwsWrapperProperty{
	Name:                "idpUsername",
	description:         "The federated user name.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IDP_PASSWORD = AwsWrapperProperty{
	Name:                "idpPassword",
	description:         "The federated user password.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IDP_PORT = AwsWrapperProperty{
	Name:                "idpPort",
	description:         "The hosting port of Identity Provider.",
	defaultValue:        "443",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IAM_ROLE_ARN = AwsWrapperProperty{
	Name:                "iamRoleArn",
	description:         "The ARN of the IAM Role that is to be assumed.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IAM_IDP_ARN = AwsWrapperProperty{
	Name:                "iamIdpArn",
	description:         "The ARN of the Identity Provider.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var IDP_ENDPOINT = AwsWrapperProperty{
	Name:                "idpEndpoint",
	description:         "The hosting URL of the Identity Provider.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var RELAYING_PARTY_ID = AwsWrapperProperty{
	Name:                "rpIdentifier",
	description:         "The relaying party identifier.",
	defaultValue:        "urn:amazon:webservices",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var DB_USER = AwsWrapperProperty{
	Name:                "dbUser",
	description:         "The database user used to access the database.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var APP_ID = AwsWrapperProperty{
	Name:                "appId",
	description:         "The ID of the AWS application configured on Okta.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var HTTP_TIMEOUT_MS = AwsWrapperProperty{
	Name:                "httpTimeoutMs",
	description:         "The timeout value in milliseconds provided to http clients.",
	defaultValue:        "60000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var SSL_INSECURE = AwsWrapperProperty{
	Name: "sslInsecure",
	description: "Set to true to disable server certificate verification. This is useful for local testing, " +
		"but setting this to true is not recommended for production environments.",
	defaultValue:        "false",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var ENABLE_TELEMETRY = AwsWrapperProperty{
	Name:                "enableTelemetry",
	description:         "Enables telemetry and observability of the wrapper.",
	defaultValue:        "false",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var TELEMETRY_SUBMIT_TOP_LEVEL = AwsWrapperProperty{
	Name:                "telemetrySubmitToplevel",
	description:         "Force submitting traces as top level traces.",
	defaultValue:        "false",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var TELEMETRY_TRACES_BACKEND = AwsWrapperProperty{
	Name:                "telemetryTracesBackend",
	description:         "Method to export telemetry traces of the wrapper.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var TELEMETRY_METRICS_BACKEND = AwsWrapperProperty{
	Name:                "telemetryMetricsBackend",
	description:         "Method to export telemetry metrics of the wrapper.",
	defaultValue:        "",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE = AwsWrapperProperty{
	Name:                "telemetryFailoverAdditionalTopTrace",
	description:         "Post an additional top-level trace for failover process.",
	defaultValue:        "false",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var LIMITLESS_MONITORING_INTERVAL_MS = AwsWrapperProperty{
	Name:                "limitlessMonitoringIntervalMs",
	description:         "Interval in milliseconds between polling for Limitless Transaction Routers to the database.",
	defaultValue:        "7500",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var LIMITLESS_MONITORING_DISPOSAL_TIME_MS = AwsWrapperProperty{
	Name:                "limitlessTransactionRouterMonitorDisposalTimeMs",
	description:         "Interval in milliseconds for a Limitless router monitor to be considered inactive and to be disposed.",
	defaultValue:        "600000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var LIMITLESS_ROUTER_CACHE_EXPIRATION_TIME_MS = AwsWrapperProperty{
	Name:                "limitlessTransactionRouterCacheExpirationTimeMs",
	description:         "Expiration in milliseconds for the Limitless router cache.",
	defaultValue:        "600000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var LIMITLESS_WAIT_FOR_ROUTER_INFO = AwsWrapperProperty{
	Name: "limitlessWaitForTransactionRouterInfo",
	description: "If the cache of transaction router info is empty and a new connection is made, this property " +
		"toggles whether the plugin will wait and synchronously fetch transaction router info before selecting a " +
		"transaction router to connect to, or to fall back to using the provided DB Shard Group endpoint URL.",
	defaultValue:        "true",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var LIMITLESS_GET_ROUTER_MAX_RETRIES = AwsWrapperProperty{
	Name:                "limitlessGetTransactionRouterInfoMaxRetries",
	description:         "Max number of connection retries fetching Limitless Transaction Router information.",
	defaultValue:        "5",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var LIMITLESS_GET_ROUTER_RETRY_INTERVAL_MS = AwsWrapperProperty{
	Name:                "limitlessGetTransactionRouterInfoRetryIntervalMs",
	description:         "Interval in milliseconds between retries fetching Limitless Transaction Router information.",
	defaultValue:        "500",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var LIMITLESS_MAX_CONN_RETRIES = AwsWrapperProperty{
	Name:                "limitlessConnectMaxRetries",
	description:         "Max number of connection retries the Limitless Plugin will attempt.",
	defaultValue:        "5",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var LIMITLESS_ROUTER_QUERY_TIMEOUT_MS = AwsWrapperProperty{
	Name:                "limitlessTransactionRouterFetchTimeoutMs",
	description:         "The timeout in milliseconds for fetching available Limitless transaction routers.",
	defaultValue:        "5000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var LIMITLESS_USE_SHARD_GROUP_URL = AwsWrapperProperty{
	Name:                "limitlessUseShardGroupUrl",
	description:         "When this parameter is set to true, provided host endpoints must be database shard group URLs. Set to false to disable this check.",
	defaultValue:        "true",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

var TRANSFER_SESSION_STATE_ON_SWITCH = AwsWrapperProperty{
	"transferSessionStateOnSwitch",
	"Enables session state transfer to a new connection.",
	"true",
	WRAPPER_TYPE_BOOL,
}

var RESET_SESSION_STATE_ON_CLOSE = AwsWrapperProperty{
	"resetSessionStateOnClose",
	"Enables resetting connection session state before closing it.",
	"true",
	WRAPPER_TYPE_BOOL,
}

var ROLLBACK_ON_SWITCH = AwsWrapperProperty{
	"rollbackOnSwitch",
	"Enables rollback of an in progress transaction when switching to a new connection.",
	"true",
	WRAPPER_TYPE_BOOL,
}

var READER_HOST_SELECTOR_STRATEGY = AwsWrapperProperty{
	Name:                "readerHostSelectorStrategy",
	description:         "The strategy that should be used to select a new reader host when opening a new connection with the rw-splitting plugin.",
	defaultValue:        "random",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var BG_CONNECT_TIMEOUT_MS = AwsWrapperProperty{
	Name:                "bgConnectTimeoutMs",
	description:         "Connect timeout in milliseconds during Blue/Green Deployment switchover.",
	defaultValue:        "30000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var BGD_ID = AwsWrapperProperty{
	Name:                "bgdId",
	description:         "Blue/Green Deployment ID",
	defaultValue:        "1",
	wrapperPropertyType: WRAPPER_TYPE_STRING,
}

var BG_INTERVAL_BASELINE_MS = AwsWrapperProperty{
	Name:                "bgBaselineMs",
	description:         "Baseline Blue/Green Deployment status checking interval in milliseconds.",
	defaultValue:        "60000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var BG_INTERVAL_INCREASED_MS = AwsWrapperProperty{
	Name:                "bgIncreasedMs",
	description:         "Increased Blue/Green Deployment status checking interval in milliseconds.",
	defaultValue:        "1000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var BG_INTERVAL_HIGH_MS = AwsWrapperProperty{
	Name:                "bgHighMs",
	description:         "High Blue/Green Deployment status checking interval in milliseconds.",
	defaultValue:        "100",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var BG_SWITCHOVER_TIMEOUT_MS = AwsWrapperProperty{
	Name:                "bgSwitchoverTimeoutMs",
	description:         "Blue/Green Deployment switchover timeout in milliseconds.",
	defaultValue:        "180000",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

var BG_SUSPEND_NEW_BLUE_CONNECTIONS = AwsWrapperProperty{
	Name:                "bgSuspendNewBlueConnections",
	description:         "Enables Blue/Green Deployment switchover to suspend new blue connection requests while the switchover process is in progress.",
	defaultValue:        "false",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
}

func RemoveInternalAwsWrapperProperties(props map[string]string) map[string]string {
	copyProps := map[string]string{}

	for key, value := range props {
		// Properties that start with monitoring/internal connect prefixes are not included in copy.
		if !startsWithPrefix(key) {
			copyProps[key] = value
		}
	}

	return copyProps
}

func startsWithPrefix(key string) bool {
	for _, prefix := range PREFIXES {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
