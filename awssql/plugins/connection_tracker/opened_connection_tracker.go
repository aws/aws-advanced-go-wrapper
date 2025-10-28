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

package connection_tracker

import (
	"database/sql/driver"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"weak"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

var openedConnections *utils.RWMap[string, *utils.RWQueue[weak.Pointer[driver.Conn]]]
var openedConnectionInitializer sync.Once

type OpenedConnectionTracker struct {
	pluginService driver_infrastructure.PluginService
}

func NewOpenedConnectionTracker(pluginService driver_infrastructure.PluginService) *OpenedConnectionTracker {
	openedConnectionInitializer.Do(func() {
		openedConnections = utils.NewRWMap[string, *utils.RWQueue[weak.Pointer[driver.Conn]]]()
	})
	return &OpenedConnectionTracker{
		pluginService: pluginService,
	}
}

func (o *OpenedConnectionTracker) PopulateOpenedConnectionQueue(hostInfo *host_info_util.HostInfo, conn driver.Conn) {
	// Check if the connection was established using an instance endpoint
	if utils.IsRdsInstance(hostInfo.GetHost()) {
		o.trackConnection(hostInfo.GetHostAndPort(), conn)
	}

	// Find the instance endpoint from aliases
	var instanceEndpoint string
	for alias := range hostInfo.GetAllAliases() {
		// Remove port from alias before checking if it's an RDS instance
		aliasWithoutPort := utils.RemovePort(alias)
		if utils.IsRdsInstance(aliasWithoutPort) {
			if alias > instanceEndpoint {
				instanceEndpoint = alias
			}
		}
	}

	if instanceEndpoint == "" {
		slog.Debug(error_util.GetMessage("OpenedConnectionTracker.unableToPopulateOpenedConnectionQueue", hostInfo.GetHost()))
		return
	}
	o.trackConnection(instanceEndpoint, conn)
}

func (o *OpenedConnectionTracker) InvalidateAllConnections(hostInfo *host_info_util.HostInfo) {
	aliases := make([]string, 0, len(hostInfo.AllAliases))
	for alias := range hostInfo.GetAllAliases() {
		aliases = append(aliases, alias)
	}
	o.InvalidateAllConnectionsMultipleHosts(hostInfo.GetHostAndPort())
	o.InvalidateAllConnectionsMultipleHosts(aliases...)
}

func (o *OpenedConnectionTracker) InvalidateAllConnectionsMultipleHosts(hosts ...string) {
	var instanceEndpoint string
	for _, host := range hosts {
		cleanHost := utils.RemovePort(host)
		if utils.IsRdsInstance(cleanHost) {
			instanceEndpoint = host
			break // Exit loop after finding first match
		}
	}
	if instanceEndpoint == "" {
		return
	}

	connectionQueue, found := openedConnections.Get(instanceEndpoint)
	if !found {
		return
	}
	o.logConnectionQueue(instanceEndpoint, connectionQueue)
	o.invalidateConnections(connectionQueue)
}

func (o *OpenedConnectionTracker) invalidateConnections(connectionQueue *utils.RWQueue[weak.Pointer[driver.Conn]]) {
	for !connectionQueue.IsEmpty() {
		if wp, ok := connectionQueue.Dequeue(); ok {
			// Try to get strong reference
			if p := wp.Value(); p != nil {
				conn := *p
				if conn != nil {
					_ = conn.Close()
				}
			}
		}
	}
}

func (o *OpenedConnectionTracker) trackConnection(hostAndPort string, conn driver.Conn) {
	connectionQueue := openedConnections.ComputeIfAbsent(hostAndPort, func() *utils.RWQueue[weak.Pointer[driver.Conn]] {
		return utils.NewRWQueue[weak.Pointer[driver.Conn]]()
	})
	connectionQueue.Enqueue(weak.Make(&conn))
	o.LogOpenedConnections()
}

func (o *OpenedConnectionTracker) logConnectionQueue(host string, queue *utils.RWQueue[weak.Pointer[driver.Conn]]) {
	if queue.IsEmpty() {
		return
	}

	debugMsg := host
	queue.ForEach(func(wp weak.Pointer[driver.Conn]) {
		debugMsg += "\n\t" + fmt.Sprintf("%v", wp)
	})

	slog.Debug(error_util.GetMessage("OpenedConnectionTracker.invalidatingConnections", debugMsg))
}

func (o *OpenedConnectionTracker) LogOpenedConnections() {
	str := ""
	hostList := []string{}

	for host, queue := range openedConnections.GetAllEntries() {
		if queue.IsEmpty() {
			continue
		}
		queue.ForEach(func(wp weak.Pointer[driver.Conn]) {
			if wp.Value() != nil {
				str += host + " "
				hostList = append(hostList, host)
			}
		})
	}
	str = strings.Join(hostList, "\n\t")

	slog.Debug("Opened Connections Tracked", "connections", str)
}

func (o *OpenedConnectionTracker) PruneNullConnections() {
	openedConnections.ForEach(func(host string, queue *utils.RWQueue[weak.Pointer[driver.Conn]]) {
		queue.RemoveIf(func(wp weak.Pointer[driver.Conn]) bool {
			return wp.Value() == nil
		})
	})
}

func (o *OpenedConnectionTracker) ClearCache() {
	openedConnections.Clear()
}

func (o *OpenedConnectionTracker) GetOpenedConnections() map[string]*utils.RWQueue[weak.Pointer[driver.Conn]] {
	return openedConnections.GetAllEntries()
}
