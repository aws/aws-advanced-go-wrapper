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

func (hostInfoBuilder *HostInfoBuilder) SetHost(host string) *HostInfoBuilder {
	hostInfoBuilder.Host = host
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetPort(port int) *HostInfoBuilder {
	hostInfoBuilder.Port = port
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetAvailability(availability HostAvailability) *HostInfoBuilder {
	hostInfoBuilder.Availability = availability
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetRole(role HostRole) *HostInfoBuilder {
	hostInfoBuilder.Role = role
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetWeight(weight int) *HostInfoBuilder {
	hostInfoBuilder.Weight = weight
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetHostId(hostId int) *HostInfoBuilder {
	hostInfoBuilder.HostId = hostId
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetLastUpdateTime(lastUpdateTime time.Time) *HostInfoBuilder {
	hostInfoBuilder.LastUpdateTime = lastUpdateTime
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) Build() (hostInfo *HostInfo) {
	// validate
	err := hostInfoBuilder.checkHostIsSet()
	if err != nil {
		panic(err)
	}

	hostInfo = &HostInfo{
		Host:           hostInfoBuilder.Host,
		HostId:         hostInfoBuilder.HostId,
		Port:           hostInfoBuilder.Port,
		Availability:   hostInfoBuilder.Availability,
		Role:           hostInfoBuilder.Role,
		Weight:         hostInfoBuilder.Weight,
		LastUpdateTime: hostInfoBuilder.LastUpdateTime,
		Aliases:        make(map[string]bool),
		AllAliases:     make(map[string]bool),
	}

	if hostInfoBuilder.Port == HOST_NO_PORT {
		hostInfo.AllAliases[hostInfoBuilder.Host] = true
	} else {
		hostInfo.AllAliases[hostInfoBuilder.Host+":"+strconv.Itoa(hostInfoBuilder.Port)] = true
	}
	return
}

func (hostInfoBuilder *HostInfoBuilder) checkHostIsSet() error {
	if hostInfoBuilder.Host == "" {
		return NewIllegalArgumentError(GetMessage("HostInfoBuilder.InvalidEmptyHost"))
	}
	return nil
}
