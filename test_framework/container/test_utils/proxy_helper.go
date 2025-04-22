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

package test_utils

import (
	"errors"
	"fmt"
	"log/slog"

	_ "github.com/Shopify/toxiproxy/client"
	toxiproxy "github.com/Shopify/toxiproxy/client"
)

func initProxies(environment *TestEnvironment) error {
	environment.proxies = map[string]ProxyInfo{}
	proxyControlPort := environment.info.proxyDatabaseInfo.controlPort
	for i, instance := range environment.info.proxyDatabaseInfo.instances {
		if instance.host == "" || instance.instanceId == "" {
			return errors.New("No valid host for instance")
		}
		client := toxiproxy.NewClient(createProxyUrl(instance.host, proxyControlPort))
		proxies, err := client.Proxies()
		if err != nil {
			return err
		}
		host := environment.info.databaseInfo.instances[i].host
		if host == "" {
			return errors.New("No host")
		}
		environment.proxies[instance.instanceId] = NewProxyInfo(proxies[environment.info.databaseInfo.instances[i].Url()], host, proxyControlPort)
	}

	endpoint := environment.info.proxyDatabaseInfo.clusterEndpoint
	if endpoint != "" {
		client := toxiproxy.NewClient(createProxyUrl(endpoint, proxyControlPort))
		baseEndpoint := environment.info.databaseInfo.clusterEndpoint
		name := fmt.Sprintf("%s:%d", baseEndpoint, environment.info.databaseInfo.clusterEndpointPort)
		proxy, err := client.Proxy(name)
		if err == nil {
			environment.proxies[endpoint] = NewProxyInfo(proxy, baseEndpoint, proxyControlPort)
		}
	}
	endpoint = environment.info.proxyDatabaseInfo.clusterReadOnlyEndpoint
	if endpoint != "" {
		client := toxiproxy.NewClient(createProxyUrl(endpoint, proxyControlPort))
		baseEndpoint := environment.info.databaseInfo.clusterReadOnlyEndpoint
		name := fmt.Sprintf("%s:%d", baseEndpoint, environment.info.databaseInfo.clusterEndpointPort)
		proxy, err := client.Proxy(name)
		if err == nil {
			environment.proxies[endpoint] = NewProxyInfo(proxy, baseEndpoint, proxyControlPort)
		}
	}
	return nil
}

func EnableAllConnectivity() {
	env, err := GetCurrentTestEnvironment()
	if err == nil {
		for _, proxy := range env.proxies {
			EnableProxyConnectivity(proxy)
		}
	}
}

func EnableProxyConnectivity(proxyInfo ProxyInfo) {
	proxy := proxyInfo.proxy
	if proxy != nil {
		err := proxy.Enable()
		if err != nil {
			slog.Debug(fmt.Sprintf("Error enabling proxy %s:%d: %s.", proxyInfo.controlHost, proxyInfo.controlPort, err.Error()))
		}
	}
}

func DisableAllConnectivity() {
	env, err := GetCurrentTestEnvironment()
	if err == nil {
		for _, proxy := range env.proxies {
			DisableProxyConnectivity(proxy)
		}
	}
}

func DisableProxyConnectivity(proxyInfo ProxyInfo) {
	proxy := proxyInfo.proxy
	if proxy != nil {
		err := proxy.Disable()
		if err != nil {
			slog.Debug(fmt.Sprintf("Error disabling proxy %s:%d: %s.", proxyInfo.controlHost, proxyInfo.controlPort, err.Error()))
		}
	}
}

func createProxyUrl(host string, port int) string {
	return fmt.Sprintf("http://%s:%d", host, port)
}
