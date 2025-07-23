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

package bg

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

const SLEEP_CHUNK_DURATION = 50 * time.Millisecond
const TELEMETRY_SWITCHOVER = "Blue/Green switchover"
const SLEEP_TIME_DURATION = 100 * time.Millisecond

type BaseRouting struct {
	hostAndPort string
	role        driver_infrastructure.BlueGreenRole
}

func NewBaseRouting(hostAndPort string, role driver_infrastructure.BlueGreenRole) BaseRouting {
	return BaseRouting{
		hostAndPort: strings.ToLower(hostAndPort),
		role:        role,
	}
}

func (b BaseRouting) Delay(delayNanos time.Duration, bgStatus driver_infrastructure.BlueGreenStatus,
	pluginService driver_infrastructure.PluginService, bgId string) {
	endTime := time.Now().Add(delayNanos)
	minDelay := min(delayNanos, SLEEP_CHUNK_DURATION)

	if bgStatus.IsZero() {
		time.Sleep(delayNanos)
	} else {
		status, ok := pluginService.GetStatus(bgId)
		for ok && bgStatus.MatchIdPhaseAndLen(status) && time.Now().Before(endTime) {
			time.Sleep(minDelay)
		}
	}
}

func (b BaseRouting) IsMatch(hostInfo *host_info_util.HostInfo, hostRole driver_infrastructure.BlueGreenRole) bool {
	hostAndPort := ""
	if !hostInfo.IsNil() {
		hostAndPort = strings.ToLower(hostInfo.GetHostAndPort())
	}
	return (b.hostAndPort == "" || b.hostAndPort == hostAndPort) && (b.role.IsZero() || b.role == hostRole)
}

func (b BaseRouting) String() string {
	hostAndPort := "<null>"
	if b.hostAndPort != "" {
		hostAndPort = b.hostAndPort
	}

	role := "<null>"
	if !b.role.IsZero() {
		role = b.role.String()
	}

	return fmt.Sprintf("%s [%s, %s]",
		"Routing",
		hostAndPort,
		role,
	)
}
