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

	toxiproxy "github.com/Shopify/toxiproxy/client"
)

func initProxies(environment *TestEnvironment) error {
	environment.proxies = map[string]ProxyInfo{}
	proxyControlPort := environment.info.ProxyDatabaseInfo.controlPort
	for i, instance := range environment.info.ProxyDatabaseInfo.Instances {
		if instance.host == "" || instance.instanceId == "" {
			return errors.New("no valid host for instance")
		}
		client := toxiproxy.NewClient(createProxyUrl(instance.host, proxyControlPort))
		proxies, err := client.Proxies()
		if err != nil {
			return err
		}
		host := environment.info.DatabaseInfo.Instances[i].host
		if host == "" {
			return errors.New("no host")
		}
		environment.proxies[instance.instanceId] = NewProxyInfo(proxies[environment.info.DatabaseInfo.Instances[i].Url()], host, proxyControlPort)
	}

	endpoint := environment.info.ProxyDatabaseInfo.ClusterEndpoint
	if endpoint != "" {
		client := toxiproxy.NewClient(createProxyUrl(endpoint, proxyControlPort))
		baseEndpoint := environment.info.DatabaseInfo.ClusterEndpoint
		name := fmt.Sprintf("%s:%d", baseEndpoint, environment.info.DatabaseInfo.ClusterEndpointPort)
		proxy, err := client.Proxy(name)
		if err == nil {
			environment.proxies[endpoint] = NewProxyInfo(proxy, baseEndpoint, proxyControlPort)
		}
	}
	endpoint = environment.info.ProxyDatabaseInfo.ClusterReadOnlyEndpoint
	if endpoint != "" {
		client := toxiproxy.NewClient(createProxyUrl(endpoint, proxyControlPort))
		baseEndpoint := environment.info.DatabaseInfo.ClusterReadOnlyEndpoint
		name := fmt.Sprintf("%s:%d", baseEndpoint, environment.info.DatabaseInfo.ClusterEndpointPort)
		proxy, err := client.Proxy(name)
		if err == nil {
			environment.proxies[endpoint] = NewProxyInfo(proxy, baseEndpoint, proxyControlPort)
		}
	}
	return nil
}

func EnableAllConnectivity(logErrors bool) {
	env, err := GetCurrentTestEnvironment()
	if err == nil {
		for _, proxy := range env.proxies {
			EnableProxyConnectivity(proxy, logErrors)
		}
	}
}

func EnableProxyConnectivity(proxyInfo ProxyInfo, logErrors bool) {
	slog.Debug(fmt.Sprintf("EnableProxyConnectivity - %s:%d", proxyInfo.controlHost, proxyInfo.controlPort))
	proxy := proxyInfo.proxy
	if proxy != nil {
		// Try to remove upstream toxic
		err := proxy.RemoveToxic("UP-STREAM")
		if err != nil && logErrors {
			slog.Debug(fmt.Sprintf("Error enabling proxy up-stream %s:%d: %s.", proxyInfo.controlHost, proxyInfo.controlPort, err.Error()))
		}

		// Try to remove downstream toxic
		err = proxy.RemoveToxic("DOWN-STREAM")
		if err != nil && logErrors {
			slog.Debug(fmt.Sprintf("Error enabling proxy down-stream %s:%d: %s.", proxyInfo.controlHost, proxyInfo.controlPort, err.Error()))
		}
	}
}

func DisableAllConnectivity() {
	slog.Debug("DisableAllConnectivity")
	env, err := GetCurrentTestEnvironment()
	if err == nil {
		for _, proxy := range env.proxies {
			DisableProxyConnectivity(proxy)
		}
	}
}

func DisableProxyConnectivity(proxyInfo ProxyInfo) {
	slog.Debug(fmt.Sprintf("DisableProxyConnectivity - %s:%d", proxyInfo.controlHost, proxyInfo.controlPort))
	proxy := proxyInfo.proxy
	if proxy != nil {
		// Add downstream toxic with 0 bandwidth.
		if _, err := proxy.AddToxic(
			"DOWN-STREAM",
			"bandwidth",
			"downstream",
			1.0,
			toxiproxy.Attributes{"rate": 0},
		); err != nil {
			slog.Debug(fmt.Sprintf("Error disabling proxy down-stream %s:%d: %s.", proxyInfo.controlHost, proxyInfo.controlPort, err.Error()))
		}

		// Add upstream toxic with 0 bandwidth.
		if _, err := proxy.AddToxic(
			"UP-STREAM",
			"bandwidth",
			"upstream",
			1.0,
			toxiproxy.Attributes{"rate": 0},
		); err != nil {
			slog.Debug(fmt.Sprintf("Error disabling proxy up-stream %s:%d: %s.", proxyInfo.controlHost, proxyInfo.controlPort, err.Error()))
		}
	}
}

func DisableAllProxies() {
	slog.Debug("DisableAllProxies")
	env, err := GetCurrentTestEnvironment()
	if err == nil {
		for _, proxy := range env.proxies {
			DisableProxy(proxy)
		}
	}
}

func DisableProxy(proxyInfo ProxyInfo) {
	proxy := proxyInfo.proxy
	if proxy != nil {
		err := proxy.Disable()
		if err != nil {
			slog.Debug(fmt.Sprintf("Error disabling proxy %s:%d: %s.", proxyInfo.controlHost, proxyInfo.controlPort, err.Error()))
		} else {
			slog.Debug(fmt.Sprintf("DisableProxy %s:%d.", proxyInfo.controlHost, proxyInfo.controlPort))
		}
	}
}

func EnableAllProxies() {
	env, err := GetCurrentTestEnvironment()
	if err == nil {
		for _, proxy := range env.proxies {
			EnableProxy(proxy)
		}
	}
}

func EnableProxy(proxyInfo ProxyInfo) {
	proxy := proxyInfo.proxy
	if proxy != nil {
		err := proxy.Enable()
		if err != nil {
			slog.Debug(fmt.Sprintf("Error enabling proxy %s:%d: %s.", proxyInfo.controlHost, proxyInfo.controlPort, err.Error()))
		} else {
			slog.Debug(fmt.Sprintf("EnableProxy %s:%d.", proxyInfo.controlHost, proxyInfo.controlPort))
		}
	}
}

func createProxyUrl(host string, port int) string {
	return fmt.Sprintf("http://%s:%d", host, port)
}
