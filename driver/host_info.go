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
func (hostInfo *HostInfo) addAlias(alias string) {
	hostInfo.Aliases[alias] = true
	hostInfo.AllAliases[alias] = true
}

//nolint:unused
func (hostInfo *HostInfo) removeAlias(alias string) {
	delete(hostInfo.Aliases, alias)
	delete(hostInfo.AllAliases, alias)
}

//nolint:unused
func (hostInfo *HostInfo) resetAliases() {
	hostInfo.Aliases = make(map[string]bool)
	hostInfo.Aliases = make(map[string]bool)

	if hostInfo.Port == HOST_NO_PORT {
		hostInfo.AllAliases[hostInfo.Host] = true
	} else {
		hostInfo.AllAliases[hostInfo.Host+":"+strconv.Itoa(hostInfo.Port)] = true
	}
}

//nolint:unused
func (hostInfo *HostInfo) getUrl() string {
	return hostInfo.getHostAndPort() + "/"
}

//nolint:unused
func (hostInfo *HostInfo) getHostAndPort() string {
	if hostInfo.isPortSpecified() {
		return hostInfo.Host + ":" + strconv.Itoa(hostInfo.Port)
	}
	return hostInfo.Host
}

//nolint:unused
func (hostInfo *HostInfo) isPortSpecified() bool {
	return hostInfo.Port != HOST_NO_PORT
}

func (hostInfo *HostInfo) Equals(host HostInfo) bool {
	return hostInfo.Host == host.Host &&
		hostInfo.Port == host.Port &&
		hostInfo.Availability == host.Availability &&
		hostInfo.Role == host.Role &&
		hostInfo.Weight == host.Weight
}

type HostInfoBuilder struct {
	host           string
	port           int
	availability   HostAvailability
	role           HostRole
	weight         int
	hostId         int
	lastUpdateTime time.Time
}

func NewHostInfoBuilder() *HostInfoBuilder {
	return &HostInfoBuilder{
		port:         HOST_NO_PORT,
		availability: AVAILABLE,
		role:         WRITER,
		weight:       HOST_DEFAULT_WEIGHT,
	}
}

func (hostInfoBuilder *HostInfoBuilder) SetHost(host string) *HostInfoBuilder {
	hostInfoBuilder.host = host
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetPort(port int) *HostInfoBuilder {
	hostInfoBuilder.port = port
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetAvailability(availability HostAvailability) *HostInfoBuilder {
	hostInfoBuilder.availability = availability
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetRole(role HostRole) *HostInfoBuilder {
	hostInfoBuilder.role = role
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetWeight(weight int) *HostInfoBuilder {
	hostInfoBuilder.weight = weight
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetHostId(hostId int) *HostInfoBuilder {
	hostInfoBuilder.hostId = hostId
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetLastUpdateTime(lastUpdateTime time.Time) *HostInfoBuilder {
	hostInfoBuilder.lastUpdateTime = lastUpdateTime
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) Build() (hostInfo *HostInfo) {
	err := hostInfoBuilder.checkHostIsSet()
	if err != nil {
		panic(err)
	}

	hostInfo = &HostInfo{
		Host:           hostInfoBuilder.host,
		HostId:         hostInfoBuilder.hostId,
		Port:           hostInfoBuilder.port,
		Availability:   hostInfoBuilder.availability,
		Role:           hostInfoBuilder.role,
		Weight:         hostInfoBuilder.weight,
		LastUpdateTime: hostInfoBuilder.lastUpdateTime,
		Aliases:        make(map[string]bool),
		AllAliases:     make(map[string]bool),
	}

	if hostInfoBuilder.port == HOST_NO_PORT {
		hostInfo.AllAliases[hostInfoBuilder.host] = true
	} else {
		hostInfo.AllAliases[hostInfoBuilder.host+":"+strconv.Itoa(hostInfoBuilder.port)] = true
	}
	return
}

func (hostInfoBuilder *HostInfoBuilder) checkHostIsSet() error {
	if hostInfoBuilder.host == "" {
		return NewIllegalArgumentError(GetMessage("HostInfoBuilder.InvalidEmptyHost"))
	}
	return nil
}
