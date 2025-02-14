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

import (
	"errors"
	"strconv"
	"time"
)

const (
	HOST_NO_PORT        = -1
	HOST_DEFAULT_WEIGHT = 100
)

type HostInfo struct {
	Host           string
	Port           int
	Availability   HostAvailability
	Role           HostRole
	Aliases        map[string]bool
	AllAliases     map[string]bool
	Weight         int
	HostId         int
	LastUpdateTime time.Time
}

//nolint:unused
func (hostSpec *HostInfo) addAlias(alias string) {
	hostSpec.Aliases[alias] = true
	hostSpec.AllAliases[alias] = true
}

//nolint:unused
func (hostSpec *HostInfo) removeAlias(alias string) {
	delete(hostSpec.Aliases, alias)
	delete(hostSpec.AllAliases, alias)
}

//nolint:unused
func (hostSpec *HostInfo) resetAliases() {
	hostSpec.Aliases = make(map[string]bool)
	hostSpec.Aliases = make(map[string]bool)

	if hostSpec.Port == HOST_NO_PORT {
		hostSpec.AllAliases[hostSpec.Host] = true
	} else {
		hostSpec.AllAliases[hostSpec.Host+":"+strconv.Itoa(hostSpec.Port)] = true
	}
}

//nolint:unused
func (hostSpec *HostInfo) getUrl() string {
	return hostSpec.getHostAndPort() + "/"
}

//nolint:unused
func (hostSpec *HostInfo) getHostAndPort() string {
	if hostSpec.isPortSpecified() {
		return hostSpec.Host + ":" + strconv.Itoa(hostSpec.Port)
	}
	return hostSpec.Host
}

//nolint:unused
func (hostSpec *HostInfo) isPortSpecified() bool {
	return hostSpec.Port != HOST_NO_PORT
}

type HostInfoBuilder struct {
	Host           string
	Port           int
	Availability   HostAvailability
	Role           HostRole
	Weight         int
	HostId         int
	LastUpdateTime time.Time
}

func NewHostInfoBuilder() *HostInfoBuilder {
	return &HostInfoBuilder{
		Port:         HOST_NO_PORT,
		Availability: AVAILABLE,
		Role:         WRITER,
		Weight:       HOST_DEFAULT_WEIGHT,
	}
}

func (hostSpecBuilder *HostInfoBuilder) SetHost(host string) *HostInfoBuilder {
	hostSpecBuilder.Host = host
	return hostSpecBuilder
}

func (hostSpecBuilder *HostInfoBuilder) SetPort(port int) *HostInfoBuilder {
	hostSpecBuilder.Port = port
	return hostSpecBuilder
}

func (hostSpecBuilder *HostInfoBuilder) SetAvailability(availability HostAvailability) *HostInfoBuilder {
	hostSpecBuilder.Availability = availability
	return hostSpecBuilder
}

func (hostSpecBuilder *HostInfoBuilder) SetRole(role HostRole) *HostInfoBuilder {
	hostSpecBuilder.Role = role
	return hostSpecBuilder
}

func (hostSpecBuilder *HostInfoBuilder) SetWeight(weight int) *HostInfoBuilder {
	hostSpecBuilder.Weight = weight
	return hostSpecBuilder
}

func (hostSpecBuilder *HostInfoBuilder) SetHostId(hostId int) *HostInfoBuilder {
	hostSpecBuilder.HostId = hostId
	return hostSpecBuilder
}

func (hostSpecBuilder *HostInfoBuilder) SetLastUpdateTime(lastUpdateTime time.Time) *HostInfoBuilder {
	hostSpecBuilder.LastUpdateTime = lastUpdateTime
	return hostSpecBuilder
}

func (hostSpecBuilder *HostInfoBuilder) Build() (hostSpec *HostInfo, err error) {
	// validate
	err = hostSpecBuilder.checkHostIsSet()

	hostSpec = &HostInfo{
		Host:           hostSpecBuilder.Host,
		HostId:         hostSpecBuilder.HostId,
		Port:           hostSpecBuilder.Port,
		Availability:   hostSpecBuilder.Availability,
		Role:           hostSpecBuilder.Role,
		Weight:         hostSpecBuilder.Weight,
		LastUpdateTime: hostSpecBuilder.LastUpdateTime,
		Aliases:        make(map[string]bool),
		AllAliases:     make(map[string]bool),
	}

	if hostSpecBuilder.Port == HOST_NO_PORT {
		hostSpec.AllAliases[hostSpecBuilder.Host] = true
	} else {
		hostSpec.AllAliases[hostSpecBuilder.Host+":"+strconv.Itoa(hostSpecBuilder.Port)] = true
	}
	return
}

func (hostSpecBuilder *HostInfoBuilder) checkHostIsSet() error {
	if hostSpecBuilder.Host == "" {
		return errors.New("TODO: import illegal argument error")
	}
	return nil
}
