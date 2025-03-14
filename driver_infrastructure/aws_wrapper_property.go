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

package driver_infrastructure

import "strconv"

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
		panic(GetMessage("AwsWrapperProperty.invalidPropertyType", property.Name, property.wrapperPropertyType))
	}

	result, ok := parsedValue.(T)
	if !ok {
		panic(GetMessage("AwsWrapperProperty.unexpectedType", property.Name, propValue))
	}

	return result
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
