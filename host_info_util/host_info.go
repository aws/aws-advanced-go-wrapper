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

package host_info_util

import (
	"awssql/error_util"
	"fmt"
	"strconv"
	"time"
)

type HostAvailability string

const (
	AVAILABLE   HostAvailability = "available"
	UNAVAILABLE HostAvailability = "unavailable"
)

type HostRole string

const (
	READER  HostRole = "reader"
	WRITER  HostRole = "writer"
	UNKNOWN HostRole = "unknown"
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

func (hostInfo *HostInfo) AddAlias(alias string) {
	hostInfo.Aliases[alias] = true
	hostInfo.AllAliases[alias] = true
}

//nolint:unused
func (hostInfo *HostInfo) removeAlias(alias string) {
	delete(hostInfo.Aliases, alias)
	delete(hostInfo.AllAliases, alias)
}

func (hostInfo *HostInfo) ResetAliases() {
	hostInfo.Aliases = make(map[string]bool)
	hostInfo.AllAliases = make(map[string]bool)

	hostInfo.AllAliases[hostInfo.GetHostAndPort()] = true
}

func (hostInfo *HostInfo) GetUrl() string {
	return hostInfo.GetHostAndPort() + "/"
}

func (hostInfo *HostInfo) GetHostAndPort() string {
	if hostInfo.IsPortSpecified() {
		return hostInfo.Host + ":" + strconv.Itoa(hostInfo.Port)
	}
	return hostInfo.Host
}

func (hostInfo *HostInfo) IsPortSpecified() bool {
	return hostInfo.Port != HOST_NO_PORT
}

func (hostInfo *HostInfo) Equals(host HostInfo) bool {
	return hostInfo.Host == host.Host &&
		hostInfo.Port == host.Port &&
		hostInfo.Availability == host.Availability &&
		hostInfo.Role == host.Role &&
		hostInfo.Weight == host.Weight
}

func (hostInfo *HostInfo) IsNil() bool {
	if hostInfo == nil {
		return true
	}
	return (hostInfo.Host == "" &&
		hostInfo.Port == 0 &&
		hostInfo.Aliases == nil &&
		hostInfo.AllAliases == nil &&
		hostInfo.HostId == 0 &&
		hostInfo.Weight == 0 &&
		hostInfo.LastUpdateTime == time.Time{})
}

func (hostInfo *HostInfo) String() string {
	return fmt.Sprintf("HostInfo[host=%s, port=%d, %s, %s, weight=%d, %s]",
		hostInfo.Host, hostInfo.Port, hostInfo.Role, hostInfo.Availability, hostInfo.Weight, hostInfo.LastUpdateTime)
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

	hostInfo.AllAliases[hostInfo.GetHostAndPort()] = true
	return
}

func (hostInfoBuilder *HostInfoBuilder) checkHostIsSet() error {
	if hostInfoBuilder.host == "" {
		return error_util.NewIllegalArgumentError(error_util.GetMessage("HostInfoBuilder.InvalidEmptyHost"))
	}
	return nil
}
