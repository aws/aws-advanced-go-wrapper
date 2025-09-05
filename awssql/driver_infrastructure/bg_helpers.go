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

package driver_infrastructure

import (
	"database/sql/driver"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

type BlueGreenIntervalRate int

const (
	INVALID BlueGreenIntervalRate = iota - 1
	BASELINE
	INCREASED
	HIGH
)

const (
	BLUE_GREEN_SOURCE string = "BLUE_GREEN_DEPLOYMENT_SOURCE"
	BLUE_GREEN_TARGET string = "BLUE_GREEN_DEPLOYMENT_TARGET"
)

const (
	AVAILABLE                     string = "AVAILABLE"
	SWITCHOVER_INITIATED          string = "SWITCHOVER_INITIATED"
	SWITCHOVER_IN_PROGRESS        string = "SWITCHOVER_IN_PROGRESS"
	SWITCHOVER_IN_POST_PROCESSING string = "SWITCHOVER_IN_POST_PROCESSING"
	SWITCHOVER_COMPLETED          string = "SWITCHOVER_COMPLETED"
)

var (
	NOT_CREATED = BlueGreenPhase{"NOT_CREATED", 0, false}
	CREATED     = BlueGreenPhase{"CREATED", 1, false}
	PREPARATION = BlueGreenPhase{"PREPARATION", 2, true}
	IN_PROGRESS = BlueGreenPhase{"IN_PROGRESS", 3, true}
	POST        = BlueGreenPhase{"POST", 4, true}
	COMPLETED   = BlueGreenPhase{"COMPLETED", 5, true}
)

var (
	SOURCE = BlueGreenRole{"SOURCE", 0}
	TARGET = BlueGreenRole{"TARGET", 1}
)

var blueGreenRoleMapping = map[string]BlueGreenRole{
	BLUE_GREEN_SOURCE: SOURCE,
	BLUE_GREEN_TARGET: TARGET,
}

var blueGreenStatusMapping = map[string]BlueGreenPhase{
	AVAILABLE:                     CREATED,
	SWITCHOVER_INITIATED:          PREPARATION,
	SWITCHOVER_IN_PROGRESS:        IN_PROGRESS,
	SWITCHOVER_IN_POST_PROCESSING: POST,
	SWITCHOVER_COMPLETED:          COMPLETED,
}

type BlueGreenPhase struct {
	name                          string
	phase                         int
	isActiveSwitchoverOrCompleted bool
}

func (b BlueGreenPhase) GetName() string {
	return b.name
}

func (b BlueGreenPhase) GetPhase() int {
	return b.phase
}

func (b BlueGreenPhase) IsActiveSwitchoverOrCompleted() bool {
	return b.isActiveSwitchoverOrCompleted
}

func (b BlueGreenPhase) IsZero() bool {
	return b.name == ""
}

func (b BlueGreenPhase) Equals(other BlueGreenPhase) bool {
	return b.name == other.name && b.phase == other.phase && b.isActiveSwitchoverOrCompleted == other.isActiveSwitchoverOrCompleted
}

func ParsePhase(statusKey string) BlueGreenPhase {
	if statusKey == "" {
		return NOT_CREATED
	}

	phase, ok := blueGreenStatusMapping[strings.ToUpper(statusKey)]
	if !ok {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.unknownStatus", statusKey))
	}
	return phase
}

type BlueGreenRole struct {
	name  string
	value int
}

func (b BlueGreenRole) GetName() string {
	return b.name
}

func (b BlueGreenRole) GetValue() int {
	return b.value
}

func ParseRole(roleKey string) BlueGreenRole {
	role, ok := blueGreenRoleMapping[strings.ToUpper(roleKey)]
	if !ok {
		slog.Debug(error_util.GetMessage("BlueGreenDeployment.unknownRole", roleKey))
	}
	return role
}

func (b BlueGreenRole) IsZero() bool {
	return b.name == "" && b.value == 0
}

func (b BlueGreenRole) String() string {
	return fmt.Sprintf("BlueGreenRole [name: %s, value: %d]", b.name, b.value)
}

type BlueGreenStatus struct {
	bgId               string
	currentPhase       BlueGreenPhase
	connectRoutings    []ConnectRouting
	executeRoutings    []ExecuteRouting
	roleByHost         *utils.RWMap[BlueGreenRole]
	correspondingHosts *utils.RWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]
}

func NewBgStatus(id string, phase BlueGreenPhase, connectRoutings []ConnectRouting, executeRoutings []ExecuteRouting,
	roleByHost *utils.RWMap[BlueGreenRole], correspondingHosts *utils.RWMap[utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo]]) BlueGreenStatus {
	return BlueGreenStatus{
		bgId:               id,
		currentPhase:       phase,
		connectRoutings:    connectRoutings,
		executeRoutings:    executeRoutings,
		roleByHost:         roleByHost,
		correspondingHosts: correspondingHosts,
	}
}

func (b BlueGreenStatus) GetCurrentPhase() BlueGreenPhase {
	return b.currentPhase
}

func (b BlueGreenStatus) GetConnectRoutings() []ConnectRouting {
	return b.connectRoutings
}

func (b BlueGreenStatus) GetExecuteRoutings() []ExecuteRouting {
	return b.executeRoutings
}

func (b BlueGreenStatus) GetBgId() string {
	return b.bgId
}

func (b BlueGreenStatus) GetCorrespondingHosts() map[string]utils.Pair[*host_info_util.HostInfo, *host_info_util.HostInfo] {
	if b.correspondingHosts == nil {
		return nil
	}
	return b.correspondingHosts.GetAllEntries()
}

func (b BlueGreenStatus) GetRole(hostInfo *host_info_util.HostInfo) (role BlueGreenRole, ok bool) {
	if hostInfo.IsNil() || b.roleByHost == nil {
		return
	}
	return b.roleByHost.Get(strings.ToLower(hostInfo.GetHost()))
}

func (b BlueGreenStatus) IsZero() bool {
	return b.bgId == "" &&
		b.currentPhase.IsZero() &&
		b.connectRoutings == nil &&
		b.executeRoutings == nil &&
		b.roleByHost == nil &&
		b.correspondingHosts == nil
}

func (b BlueGreenStatus) MatchIdPhaseAndLen(other BlueGreenStatus) bool {
	return b.bgId == other.bgId &&
		b.currentPhase == other.currentPhase &&
		len(b.connectRoutings) == len(other.connectRoutings) &&
		len(b.executeRoutings) == len(other.executeRoutings) &&
		b.roleByHost.Size() == other.roleByHost.Size() &&
		b.correspondingHosts.Size() == other.correspondingHosts.Size()
}

func (b BlueGreenStatus) String() string {
	roleByHostMapStr := "-"
	connectRoutingStr := "-"
	executeRoutingStr := "-"

	return fmt.Sprintf("BlueGreenStatus [\n"+
		"\tbgId: %s,\n"+
		"\tphase: %s,\n"+
		"\tconnect routing: %s,\n"+
		"\texecute routing: %s,\n"+
		"\troleByHost: %s\n"+
		"]",
		b.bgId, b.currentPhase.GetName(), connectRoutingStr, executeRoutingStr, roleByHostMapStr)
}

type ConnectRouting interface {
	IsMatch(hostInfo *host_info_util.HostInfo, hostRole BlueGreenRole) bool

	Apply(
		plugin ConnectionPlugin,
		hostInfo *host_info_util.HostInfo,
		properties map[string]string,
		isInitialConnection bool,
		pluginService PluginService,
	) (driver.Conn, error)
}

type ExecuteRouting interface {
	IsMatch(hostInfo *host_info_util.HostInfo, hostRole BlueGreenRole) bool
	Apply(
		plugin ConnectionPlugin,
		properties map[string]string,
		pluginService PluginService,
		methodName string,
		methodFunc ExecuteFunc,
		methodArgs ...any,
	) RoutingResultHolder
}

type BlueGreenResult struct {
	Version  string
	Endpoint string
	Port     int
	Role     string
	Status   string
}

func (b *BlueGreenResult) String() string {
	return fmt.Sprintf("BlueGreenResult [\n"+
		"\tversion: %s,\n"+
		"\tendpoint: %s,\n"+
		"\tport routing: %d,\n"+
		"\trole routing: %s,\n"+
		"\tstatus: %s\n"+
		"]",
		b.Version, b.Endpoint, b.Port, b.Role, b.Status)
}

type EmptyResult struct{}

type RoutingResultHolder struct {
	WrappedReturnValue  any
	WrappedReturnValue2 any
	WrappedOk           bool
	WrappedErr          error
}

var EMPTY_VAL = EmptyResult{}
var EMPTY_ROUTING_RESULT_HOLDER = RoutingResultHolder{WrappedReturnValue: EMPTY_VAL}

func (r RoutingResultHolder) GetResult() (any, any, bool, error) {
	return r.WrappedReturnValue, r.WrappedReturnValue2, r.WrappedOk, r.WrappedErr
}

func (r RoutingResultHolder) IsPresent() bool {
	return r != EMPTY_ROUTING_RESULT_HOLDER
}
