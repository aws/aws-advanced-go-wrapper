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

//import (
//	"awssql/driver_infrastructure"
//	"awssql/container"
//	"awssql/plugin_helpers"
//	"reflect"
//	"testing"
//)
//
//// TODO: complete once additional plugins have been added.
//func TestSortPlugins(t *testing.T) {
//	builder := &container.ConnectionPluginChainBuilder{}
//	props := map[string]any{}
//	props[driver_infrastructure.PLUGINS.Name] = "iam,efm,failover"
//	pluginManagerImpl := plugin_helpers.PluginManagerImpl{}
//	pluginManager := driver_infrastructure.PluginManager(&pluginManagerImpl)
//	var driver driver_infrastructure.PluginService = plugin_helpers.NewPluginServiceImpl(&pluginManagerImpl, props)
//	plugins, err := builder.GetPlugins(&driver, &pluginManager, props)
//
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(plugins) != 4 {
//		t.Fatal("Expected 4 plugins.")
//	}
//
//	_, failoverOk := (*plugins[0]).(plugins.FailoverPlugin)
//	if !failoverOk {
//		t.Fatal("Expected first plugin to be a FailoverPlugin.")
//	}
//
//	_, efmOk := (*plugins[1]).(plugins.HostMonitoringPlugin)
//	if !efmOk {
//		t.Fatal("Expected second plugin to be a HostMonitoringPlugin.")
//	}
//
//	_, iamOk := (*plugins[2]).(plugins.IamAuthenticationPlugin)
//	if !iamOk {
//		t.Fatal("Expected third plugin to be a IamAuthenticationPlugin.")
//	}
//
//	if reflect.TypeOf(*plugins[3]).String() != "driver_infrastructure.DefaultPlugin" {
//		t.Fatal("Expected fourth plugin to be a DefaultPlugin.")
//	}
//}
//
//func TestPreservePluginOrder(t *testing.T) {
//	builder := &container.ConnectionPluginChainBuilder{}
//	props := map[string]any{}
//	props[driver_infrastructure.PLUGINS.Name] = "iam,efm,failover"
//	props[driver_infrastructure.AUTO_SORT_PLUGIN_ORDER.Name] = false
//	pluginManagerImpl := plugin_helpers.PluginManagerImpl{}
//	pluginManager := driver_infrastructure.PluginManager(&pluginManagerImpl)
//	var driver driver_infrastructure.PluginService = plugin_helpers.NewPluginServiceImpl(&pluginManagerImpl, props)
//	plugins, err := builder.GetPlugins(&driver, &pluginManager, props)
//
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(plugins) != 4 {
//		t.Fatal("Expected 4 plugins.")
//	}
//
//	_, iamOk := (*plugins[0]).(plugins.IamAuthenticationPlugin)
//	if !iamOk {
//		t.Fatal("Expected first plugin to be a IamAuthenticationPlugin.")
//	}
//
//	_, efmOk := (*plugins[1]).(plugins.HostMonitoringPlugin)
//	if !efmOk {
//		t.Fatal("Expected second plugin to be a HostMonitoringPlugin.")
//	}
//
//	_, failoverOk := (*plugins[2]).(plugins.FailoverPlugin)
//	if !failoverOk {
//		t.Fatal("Expected third plugin to be a FailoverPlugin.")
//	}
//
//	if reflect.TypeOf(plugins[3]).String() != "driver_infrastructure.DefaultPlugin" {
//		t.Fatal("Expected fourth plugin to be a DefaultPlugin.")
//	}
//}
//
//func TestSortPluginsWithStickToPrior(t *testing.T) {
//	builder := &container.ConnectionPluginChainBuilder{}
//	props := map[string]any{}
//	props[driver_infrastructure.PLUGINS.Name] = "dev,iam,executionTime,connectTime,efm,failover"
//	pluginManagerImpl := plugin_helpers.PluginManagerImpl{}
//	pluginManager := driver_infrastructure.PluginManager(&pluginManagerImpl)
//	var driver driver_infrastructure.PluginService = plugin_helpers.NewPluginServiceImpl(&pluginManagerImpl, props)
//	plugins, err := builder.GetPlugins(&driver, &pluginManager, props)
//
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(plugins) != 7 {
//		t.Fatal("Expected 7 plugins.")
//	}
//
//	_, devOk := (*plugins[0]).(plugins.DeveloperPlugin)
//	if !devOk {
//		t.Fatal("Expected first plugin to be a DeveloperPlugin.")
//	}
//
//	_, efmOk := (*plugins[1]).(plugins.FailoverPlugin)
//	if !efmOk {
//		t.Fatal("Expected second plugin to be a FailoverPlugin.")
//	}
//
//	_, failoverOk := (*plugins[2]).(plugins.HostMonitoringPlugin)
//	if !failoverOk {
//		t.Fatal("Expected third plugin to be a HostMonitoringPlugin.")
//	}
//
//	_, iamOk := (*plugins[3]).(plugins.IamAuthenticationPlugin)
//	if !iamOk {
//		t.Fatal("Expected fourth plugin to be a IamAuthenticationPlugin.")
//	}
//
//	_, executeOk := (*plugins[4]).(plugins.ExecuteTimePlugin)
//	if !executeOk {
//		t.Fatal("Expected fifth plugin to be a ExecuteTimePlugin.")
//	}
//
//	_, connectOk := (*plugins[5]).(plugins.ConnectTimePlugin)
//	if !connectOk {
//		t.Fatal("Expected sixth plugin to be a ConnectTimePlugin.")
//	}
//
//	if reflect.TypeOf(plugins[6]).String() != "driver_infrastructure.DefaultPlugin" {
//		t.Fatal("Expected seventh plugin to be a DefaultPlugin.")
//	}
//}
