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

// TODO: complete once additional plugins have been added.
//func TestSortPlugins(t *testing.T) {
//	builder := &ConnectionPluginChainBuilder{}
//	props := map[string]any{}
//	props[PLUGINS.name] = "iam,efm,failover"
//	pluginManager := PluginManager{}
//	var foo PluginService = NewPluginServiceImpl(pluginManager, props)
//	plugins, err := builder.GetPlugins(&foo, pluginManager, props)
//
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(plugins) != 4 {
//		t.Fatal("Expected 4 plugins.")
//	}
//
//	_, failoverOk := plugins[0].(FailoverPlugin)
//	if !failoverOk {
//		t.Fatal("Expected first plugin to be a FailoverPlugin.")
//	}
//
//	_, efmOk := plugins[1].(HostMonitoringPlugin)
//	if !efmOk {
//		t.Fatal("Expected second plugin to be a HostMonitoringPlugin.")
//	}
//
//	_, iamOk := plugins[2].(IamAuthenticationPlugin)
//	if !iamOk {
//		t.Fatal("Expected third plugin to be a IamAuthenticationPlugin.")
//	}
//
//	if reflect.TypeOf(plugins[3]).String() != "driver.DefaultPlugin" {
//		t.Fatal("Expected fourth plugin to be a DefaultPlugin.")
//	}
//}
//
//func TestPreservePluginOrder(t *testing.T) {
//	builder := &ConnectionPluginChainBuilder{}
//	props := map[string]any{}
//	props[PLUGINS.name] = "iam,efm,failover"
//	props[AUTO_SORT_PLUGIN_ORDER.name] = false
//	pluginManager := PluginManager{}
//	var foo PluginService = NewPluginServiceImpl(pluginManager, props)
//	plugins, err := builder.GetPlugins(&foo, pluginManager, props)
//
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(plugins) != 4 {
//		t.Fatal("Expected 4 plugins.")
//	}
//
//	_, iamOk := plugins[0].(IamAuthenticationPlugin)
//	if !iamOk {
//		t.Fatal("Expected first plugin to be a IamAuthenticationPlugin.")
//	}
//
//	_, efmOk := plugins[1].(HostMonitoringPlugin)
//	if !efmOk {
//		t.Fatal("Expected second plugin to be a HostMonitoringPlugin.")
//	}
//
//	_, failoverOk := plugins[2].(FailoverPlugin)
//	if !failoverOk {
//		t.Fatal("Expected third plugin to be a FailoverPlugin.")
//	}
//
//	if reflect.TypeOf(plugins[3]).String() != "driver.DefaultPlugin" {
//		t.Fatal("Expected fourth plugin to be a DefaultPlugin.")
//	}
//}
//
//func TestSortPluginsWithStickToPrior(t *testing.T) {
//	builder := &ConnectionPluginChainBuilder{}
//	props := map[string]any{}
//	props[PLUGINS.name] = "dev,iam,executionTime,connectTime,efm,failover"
//	pluginManager := PluginManager{}
//	var foo PluginService = NewPluginServiceImpl(pluginManager, props)
//	plugins, err := builder.GetPlugins(&foo, pluginManager, props)
//
//	if err != nil {
//		t.Fatal(err)
//	}
//	if len(plugins) != 7 {
//		t.Fatal("Expected 7 plugins.")
//	}
//
//	_, devOk := plugins[0].(DeveloperPlugin)
//	if !devOk {
//		t.Fatal("Expected first plugin to be a DeveloperPlugin.")
//	}
//
//	_, efmOk := plugins[1].(FailoverPlugin)
//	if !efmOk {
//		t.Fatal("Expected second plugin to be a FailoverPlugin.")
//	}
//
//	_, failoverOk := plugins[2].(HostMonitoringPlugin)
//	if !failoverOk {
//		t.Fatal("Expected third plugin to be a HostMonitoringPlugin.")
//	}
//
//	_, iamOk := plugins[3].(IamAuthenticationPlugin)
//	if !iamOk {
//		t.Fatal("Expected fourth plugin to be a IamAuthenticationPlugin.")
//	}
//
//	_, executeOk := plugins[4].(ExecuteTimePlugin)
//	if !executeOk {
//		t.Fatal("Expected fifth plugin to be a ExecuteTimePlugin.")
//	}
//
//	_, connectOk := plugins[5].(ConnectTimePlugin)
//	if !connectOk {
//		t.Fatal("Expected sixth plugin to be a ConnectTimePlugin.")
//	}
//
//	if reflect.TypeOf(plugins[6]).String() != "driver.DefaultPlugin" {
//		t.Fatal("Expected seventh plugin to be a DefaultPlugin.")
//	}
//}
