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
	"awssql/error_util"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

const DEFAULT_PLUGINS = "efm"
const MONITORING_PROPERTY_PREFIX = "monitoring-"

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

func GetVerifiedWrapperPropertyValue[T any](props map[string]string, property AwsWrapperProperty) (result T, err error) {
	propValue := property.Get(props)
	var parsedValue any
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
		return result, error_util.NewGenericAwsWrapperError(error_util.GetMessage("AwsWrapperProperty.unexpectedType", property.Name, propValue))
	}

	return result, nil
}

var ALL_WRAPPER_PROPERTIES = map[string]bool{
	USER.Name:                             true,
	PASSWORD.Name:                         true,
	HOST.Name:                             true,
	PORT.Name:                             true,
	DATABASE.Name:                         true,
	DRIVER_PROTOCOL.Name:                  true,
	NET.Name:                              true,
	SINGLE_WRITER_DSN.Name:                true,
	PLUGINS.Name:                          true,
	AUTO_SORT_PLUGIN_ORDER.Name:           true,
	TARGET_DRIVER_DIALECT.Name:            true,
	TARGET_DRIVER_AUTO_REGISTER.Name:      true,
	CLUSTER_TOPOLOGY_REFRESH_RATE_MS.Name: true,
	CLUSTER_ID.Name:                       true,
	CLUSTER_INSTANCE_HOST_PATTERN.Name:    true,
	IAM_HOST.Name:                         true,
	IAM_EXPIRATION.Name:                   true,
	IAM_REGION.Name:                       true,
	IAM_DEFAULT_PORT.Name:                 true,
	FAILURE_DETECTION_ENABLED.Name:        true,
	FAILURE_DETECTION_TIME_MS.Name:        true,
	FAILURE_DETECTION_INTERVAL_MS.Name:    true,
	FAILURE_DETECTION_COUNT.Name:          true,
	MONITOR_DISPOSAL_TIME_MS.Name:         true,
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

var FAILURE_DETECTION_ENABLED = AwsWrapperProperty{
	Name:                "failureDetectionEnabled",
	description:         "Enable failure detection logic (aka host monitoring thread).",
	defaultValue:        "true",
	wrapperPropertyType: WRAPPER_TYPE_BOOL,
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

var IAM_EXPIRATION = AwsWrapperProperty{
	Name:                "iamExpiration",
	description:         "IAM token cache expiration in seconds.",
	defaultValue:        "870",
	wrapperPropertyType: WRAPPER_TYPE_INT,
}

func RemoveMonitoringProperties(props map[string]string) map[string]string {
	copyProps := map[string]string{}

	for key, value := range props {
		// Monitoring properties are not included in copy.
		if !strings.HasPrefix(key, MONITORING_PROPERTY_PREFIX) {
			copyProps[key] = value
		}
	}

	return copyProps
}
