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
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
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
	HostId         string
	Port           int
	Availability   HostAvailability
	Role           HostRole
	Aliases        map[string]bool
	AllAliases     map[string]bool
	Weight         int
	LastUpdateTime time.Time
}

func (hostInfo *HostInfo) AddAlias(alias string) {
	if alias != "" && hostInfo != nil {
		hostInfo.Aliases[alias] = true
		hostInfo.AllAliases[alias] = true
	}
}

func (hostInfo *HostInfo) ResetAliases() {
	hostInfo.Aliases = make(map[string]bool)
	hostInfo.AllAliases = make(map[string]bool)

	hostInfo.AllAliases[hostInfo.GetHostAndPort()] = true
}

func (hostInfo *HostInfo) GetAliases() map[string]bool {
	if hostInfo == nil {
		return nil
	}
	return hostInfo.Aliases
}

func (hostInfo *HostInfo) GetAllAliases() map[string]bool {
	if hostInfo == nil {
		return nil
	}
	return hostInfo.AllAliases
}

func (hostInfo *HostInfo) GetUrl() string {
	return hostInfo.GetHostAndPort() + "/"
}

func (hostInfo *HostInfo) GetHost() string {
	if hostInfo == nil {
		return ""
	}
	return hostInfo.Host
}

func (hostInfo *HostInfo) GetHostAndPort() string {
	if hostInfo == nil {
		return ""
	}
	if hostInfo.IsPortSpecified() {
		return hostInfo.Host + ":" + strconv.Itoa(hostInfo.Port)
	}
	return hostInfo.Host
}

func (hostInfo *HostInfo) IsPortSpecified() bool {
	return hostInfo != nil && hostInfo.Port != HOST_NO_PORT
}

func (hostInfo *HostInfo) Equals(host *HostInfo) bool {
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
	return hostInfo.Host == "" &&
		hostInfo.Port == 0 &&
		hostInfo.Aliases == nil &&
		hostInfo.AllAliases == nil &&
		hostInfo.HostId == "" &&
		hostInfo.Weight == 0 &&
		hostInfo.LastUpdateTime.Equal(time.Time{})
}

func (hostInfo *HostInfo) String() string {
	return fmt.Sprintf("HostInfo[hostId=%s,host=%s, port=%d, %s, %s, weight=%d, %s]",
		hostInfo.HostId, hostInfo.Host, hostInfo.Port, hostInfo.Role, hostInfo.Availability, hostInfo.Weight, hostInfo.LastUpdateTime)
}

func (hostInfo *HostInfo) MakeCopyWithRole(role HostRole) *HostInfo {
	return &HostInfo{
		Host:           hostInfo.Host,
		HostId:         hostInfo.HostId,
		Port:           hostInfo.Port,
		Availability:   hostInfo.Availability,
		Role:           role,
		Aliases:        hostInfo.Aliases,
		AllAliases:     hostInfo.AllAliases,
		Weight:         hostInfo.Weight,
		LastUpdateTime: hostInfo.LastUpdateTime,
	}
}

type HostInfoBuilder struct {
	host           string
	hostId         string
	port           int
	availability   HostAvailability
	role           HostRole
	weight         int
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

func (hostInfoBuilder *HostInfoBuilder) SetHostId(hostId string) *HostInfoBuilder {
	hostInfoBuilder.hostId = hostId
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) SetLastUpdateTime(lastUpdateTime time.Time) *HostInfoBuilder {
	hostInfoBuilder.lastUpdateTime = lastUpdateTime
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) CopyFrom(hostInfo *HostInfo) *HostInfoBuilder {
	if hostInfo != nil {
		hostInfoBuilder.host = hostInfo.Host
		hostInfoBuilder.hostId = hostInfo.HostId
		hostInfoBuilder.port = hostInfo.Port
		hostInfoBuilder.availability = hostInfo.Availability
		hostInfoBuilder.role = hostInfo.Role
		hostInfoBuilder.weight = hostInfo.Weight
		hostInfoBuilder.lastUpdateTime = hostInfo.LastUpdateTime
	}
	return hostInfoBuilder
}

func (hostInfoBuilder *HostInfoBuilder) Build() (hostInfo *HostInfo, err error) {
	err = hostInfoBuilder.checkHostIsSet()
	if err != nil {
		return
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
