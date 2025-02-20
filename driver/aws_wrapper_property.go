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

package driver

type AwsWrapperProperty struct {
	name         string
	description  string
	defaultValue any
}

func (prop *AwsWrapperProperty) Get(props map[string]any) any {
	var result, ok = props[prop.name]
	if !ok {
		return prop.defaultValue
	}
	return result
}

func (prop *AwsWrapperProperty) Set(props map[string]any, val any) {
	props[prop.name] = val
}

var SINGLE_WRITER_CONNECTION_STRING = AwsWrapperProperty{
	"singleWriterConnectionString",
	"Set to true if you are providing a connection string with multiple comma-delimited hosts and your cluster has only one writer. The writer must be the first host in the connection string",
	false,
}
