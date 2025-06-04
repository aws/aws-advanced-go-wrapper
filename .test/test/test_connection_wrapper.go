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

package test

import (
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
)

type TestConnectionWrapper struct {
	driver.AwsWrapperConn
	PluginManager driver_infrastructure.PluginManager
	PluginService driver_infrastructure.PluginService
}

func NewTestConnectionWrapper(
	props map[string]string, // TODO
	pluginManager driver_infrastructure.PluginManager,
	pluginService driver_infrastructure.PluginService,
	pluginManagerProvider driver_infrastructure.PluginManagerProvider,
	pluginServiceProvider driver_infrastructure.PluginServiceProvider,
	dsn string,
	dbEngine driver_infrastructure.DatabaseEngine,
) *TestConnectionWrapper {
	return &TestConnectionWrapper{
		AwsWrapperConn: *driver.NewAwsWrapperConn(pluginManager, pluginService, dbEngine),
		PluginManager:  pluginManager,
		PluginService:  pluginService,
	}
}
