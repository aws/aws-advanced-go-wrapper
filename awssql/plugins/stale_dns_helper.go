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

package plugins

import (
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils/telemetry"
)

type StaleDnsHelper struct {
	pluginService   driver_infrastructure.PluginService
	staleDnsCounter telemetry.TelemetryCounter
	writerHostInfo  *host_info_util.HostInfo
	writerHostAddr  string
}

func NewStaleDnsHelper(pluginService driver_infrastructure.PluginService) (*StaleDnsHelper, error) {
	staleDnsCounter, err := pluginService.GetTelemetryFactory().CreateCounter("staleDNS.stale.detected")
	if err != nil {
		return nil, err
	}
	return &StaleDnsHelper{pluginService: pluginService, staleDnsCounter: staleDnsCounter}, nil
}

func (s *StaleDnsHelper) GetVerifiedConnection(
	host string,
	isInitialConnection bool,
	hostListProviderService driver_infrastructure.HostListProviderService,
	props *utils.RWMap[string, string],
	connectFunc driver_infrastructure.ConnectFunc) (driver.Conn, error) {
	conn, err := connectFunc(props)

	if !utils.IsWriterClusterDns(host) {
		return conn, err
	}

	if err != nil {
		return nil, err
	}

	ip, ipErr := net.LookupIP(host)
	clusterInetAddress := ""
	if ipErr == nil {
		clusterInetAddress = fmt.Sprintf("%s", ip)
		clusterInetAddress = clusterInetAddress[1 : len(clusterInetAddress)-1]
	}

	hostInetAddress := clusterInetAddress
	slog.Info(error_util.GetMessage("StaleDnsHelper.clusterEndpointDns", hostInetAddress))

	if hostInetAddress == "" {
		return conn, err
	}

	if s.pluginService.GetHostRole(conn) == host_info_util.READER {
		// This if-statement is only reached if the connection url is a writer cluster endpoint.
		// If the new connection resolves to a reader instance, this means the topology is outdated.
		// Force refresh to update the topology.
		err = s.pluginService.ForceRefreshHostList(conn)
		if err != nil {
			return nil, err
		}
	} else {
		err = s.pluginService.RefreshHostList(conn)
		if err != nil {
			return nil, err
		}
	}

	slog.Info(utils.LogTopology(s.pluginService.GetHosts(), "[StaleDnsHelper.getVerifiedConnection]"))

	if s.writerHostInfo.IsNil() {
		writerCandidate := host_info_util.GetWriter(s.pluginService.GetHosts())
		if !writerCandidate.IsNil() && utils.IsRdsClusterDns(writerCandidate.GetHost()) {
			return nil, nil
		}
		s.writerHostInfo = writerCandidate
	}

	slog.Info(error_util.GetMessage("StaleDnsHelper.writerHostInfo", s.writerHostInfo.String()))

	if s.writerHostInfo.IsNil() {
		return conn, nil
	}

	if s.writerHostAddr == "" {
		ip, ipErr = net.LookupIP(s.writerHostInfo.Host)
		if ipErr == nil {
			s.writerHostAddr = fmt.Sprintf("%s", ip)
			s.writerHostAddr = s.writerHostAddr[1 : len(s.writerHostAddr)-1]
		}
	}

	slog.Info(error_util.GetMessage("StaleDnsHelper.writerInetAddress", s.writerHostAddr))

	if s.writerHostAddr == "" {
		return conn, nil
	}

	if s.writerHostAddr != clusterInetAddress {
		// DNS resolves a cluster endpoint to a wrong writer opens a connection to a proper writer host

		slog.Info(error_util.GetMessage("StaleDnsHelper.staleDnsDetected", s.writerHostInfo.String()))

		s.staleDnsCounter.Inc(s.pluginService.GetTelemetryContext())

		writerConn, connectErr := s.pluginService.Connect(s.writerHostInfo, props, nil)
		if connectErr != nil {
			return nil, connectErr
		}

		if isInitialConnection {
			hostListProviderService.SetInitialConnectionHostInfo(s.writerHostInfo)
		}

		if conn != nil {
			_ = conn.Close()
			return writerConn, nil
		}
	}

	return conn, nil
}
