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
	"strconv"
)

const DEFAULT_PLUGINS = ""

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
	switch property.wrapperPropertyType {
	case WRAPPER_TYPE_INT:
		parsedValue, _ = strconv.Atoi(propValue)
	case WRAPPER_TYPE_STRING:
		parsedValue = propValue
	case WRAPPER_TYPE_BOOL:
		parsedValue, _ = strconv.ParseBool(propValue)
	default:
		panic(error_util.GetMessage("AwsWrapperProperty.invalidPropertyType", property.Name, property.wrapperPropertyType))
	}

	result, ok := parsedValue.(T)
	if !ok {
		panic(error_util.GetMessage("AwsWrapperProperty.unexpectedType", property.Name, propValue))
	}

	return result
}

var ALL_WRAPPER_PROPERTIES = map[string]bool{
	USER.Name:                   true,
	PASSWORD.Name:               true,
	HOST.Name:                   true,
	PORT.Name:                   true,
	DATABASE.Name:               true,
	DRIVER_PROTOCOL.Name:        true,
	NET.Name:                    true,
	SINGLE_WRITER_DSN.Name:      true,
	PLUGINS.Name:                true,
	AUTO_SORT_PLUGIN_ORDER.Name: true,
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
	"singleWriterDsn",
	"Set to true if you are providing a dsn with multiple comma-delimited hosts and your cluster has only one writer. The writer must be the first host in the dsn.",
	"false",
	WRAPPER_TYPE_BOOL,
}

var PLUGINS = AwsWrapperProperty{
	"plugins",
	"Comma separated list of connection plugin codes.",
	DEFAULT_PLUGINS,
	WRAPPER_TYPE_STRING,
}

var AUTO_SORT_PLUGIN_ORDER = AwsWrapperProperty{
	"autoSortPluginOrder",
	"This flag is enabled by default, meaning that the plugins order will be automatically adjusted. Disable it at your own " +
		"risk or if you really need plugins to be executed in a particular order.",
	"true",
	WRAPPER_TYPE_BOOL,
}

var TARGET_DRIVER_DIALECT = AwsWrapperProperty{
	"targetDriverDialect",
	"A unique identifier for the target driver dialect.",
	"",
	WRAPPER_TYPE_STRING,
}

var TARGET_DRIVER_AUTO_REGISTER = AwsWrapperProperty{
	"targetDriverAutoRegister",
	"Allows the AWS Advanced Go Wrapper to auto-register a target driver.",
	"true",
	WRAPPER_TYPE_BOOL,
}
