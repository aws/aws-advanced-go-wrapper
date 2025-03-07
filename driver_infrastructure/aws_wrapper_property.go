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

const DEFAULT_PLUGINS = ""

type AwsWrapperProperty struct {
	Name         string
	description  string
	defaultValue any
}

func (prop *AwsWrapperProperty) Get(props map[string]any) any {
	var result, ok = props[prop.Name]
	if !ok {
		return prop.defaultValue
	}
	return result
}

func (prop *AwsWrapperProperty) Set(props map[string]any, val any) {
	props[prop.Name] = val
}

var PLUGINS = AwsWrapperProperty{
	"plugins",
	"Comma separated list of connection plugin codes.",
	DEFAULT_PLUGINS,
}

var AUTO_SORT_PLUGIN_ORDER = AwsWrapperProperty{
	"autoSortPluginOrder",
	"This flag is enabled by default, meaning that the plugins order will be automatically adjusted. Disable it at your own " +
		"risk or if you really need plugins to be executed in a particular order.",
	true,
}
