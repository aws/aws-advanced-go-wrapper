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
	"context"
	"database/sql/driver"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

const DefaultQueryTimeoutMs = 1000

// =============================================================================
// Interface
// =============================================================================

// TopologyUtils defines the interface for querying and processing database topology information.
type TopologyUtils interface {
	QueryForTopology(conn driver.Conn, initialHost *host_info_util.HostInfo, instanceTemplate *host_info_util.HostInfo) ([]*host_info_util.HostInfo, error)
	GetHostRole(conn driver.Conn) host_info_util.HostRole
	// GetInstanceId returns the instance identifier for the connected database instance.
	// Returns (instanceId, instanceName) - empty strings if unable to determine.
	GetInstanceId(conn driver.Conn) (string, string)
	IsWriterInstance(conn driver.Conn) (bool, error)
	// CreateHost creates a HostInfo from topology data. Returns nil if creation fails.
	CreateHost(
		instanceId, instanceName string, isWriter bool, weight int,
		lastUpdateTime time.Time, initialHost, instanceTemplate *host_info_util.HostInfo,
	) *host_info_util.HostInfo
}

// =============================================================================
// Shared Helper Functions
// =============================================================================

func executeQuery(conn driver.Conn, query string) (driver.Rows, error) {
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
	}
	return queryerCtx.QueryContext(context.Background(), query, nil)
}

func verifyWriter(hosts []*host_info_util.HostInfo) []*host_info_util.HostInfo {
	if len(hosts) == 0 {
		return nil
	}

	var writers, readers []*host_info_util.HostInfo
	for _, host := range hosts {
		if host.Role == host_info_util.WRITER {
			writers = append(writers, host)
		} else {
			readers = append(readers, host)
		}
	}

	if len(writers) == 0 {
		return nil
	}

	selectedWriter := writers[0]
	if len(writers) > 1 {
		sort.Slice(writers, func(i, j int) bool {
			return writers[i].LastUpdateTime.After(writers[j].LastUpdateTime)
		})
		selectedWriter = writers[0]
	}

	result := make([]*host_info_util.HostInfo, 0, len(readers)+1)
	result = append(result, selectedWriter)
	return append(result, readers...)
}

// CreateHost creates a HostInfo from topology data. Returns nil if creation fails.
func CreateHost(
	instanceId, instanceName string,
	isWriter bool,
	weight int,
	lastUpdateTime time.Time,
	initialHost, instanceTemplate *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	if instanceName == "" {
		instanceName = "?"
	}

	endpoint := strings.Replace(instanceTemplate.Host, "?", instanceName, 1)
	port := instanceTemplate.Port
	if port == host_info_util.HOST_NO_PORT && initialHost != nil && initialHost.Port != host_info_util.HOST_NO_PORT {
		port = initialHost.Port
	}

	role := host_info_util.READER
	if isWriter {
		role = host_info_util.WRITER
	}

	hostInfo, err := host_info_util.NewHostInfoBuilder().
		SetHostId(instanceId).
		SetHost(endpoint).
		SetPort(port).
		SetRole(role).
		SetAvailability(host_info_util.AVAILABLE).
		SetWeight(weight).
		SetLastUpdateTime(lastUpdateTime).
		Build()
	if err == nil {
		hostInfo.AddAlias(instanceName)
	}
	return hostInfo
}

// =============================================================================
// Shared topology query helpers
// =============================================================================

// queryHostRole executes the is-reader query and returns the host role.
func queryHostRole(conn driver.Conn, query string, parser RowParser) host_info_util.HostRole {
	rows, err := executeQuery(conn, query)
	if err != nil {
		return host_info_util.UNKNOWN
	}
	defer rows.Close()

	row := make([]driver.Value, 1)
	if rows.Next(row) == nil && len(row) > 0 {
		if isReader, ok := parser.ParseBool(row[0]); ok {
			if isReader {
				return host_info_util.READER
			}
			return host_info_util.WRITER
		}
	}
	return host_info_util.UNKNOWN
}

// queryInstanceId executes the instance ID query and returns (id, name).
// Automatically handles single-column queries (returns id, id) and
// two-column queries (returns id, name) based on the result set.
// Returns empty strings if unable to determine.
func queryInstanceId(conn driver.Conn, query string, parser RowParser) (string, string) {
	rows, err := executeQuery(conn, query)
	if err != nil {
		return "", ""
	}
	defer rows.Close()

	numCols := len(rows.Columns())
	row := make([]driver.Value, numCols)
	if rows.Next(row) != nil {
		return "", ""
	}

	instanceId, ok := parser.ParseString(row[0])
	if !ok {
		return "", ""
	}

	if numCols < 2 {
		return instanceId, instanceId
	}

	instanceName, _ := parser.ParseString(row[1])
	return instanceId, instanceName
}

// queryIsWriter executes the writer ID query and checks if connected to writer.
// For Aurora: writer if query returns non-empty server ID.
// For Multi-AZ: writer if query returns empty (no rows).
func queryIsWriter(conn driver.Conn, query string, parser RowParser, writerWhenEmpty bool) (bool, error) {
	rows, err := executeQuery(conn, query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	row := make([]driver.Value, 1)
	hasRow := rows.Next(row) == nil

	if writerWhenEmpty {
		return !hasRow, nil
	}

	if hasRow {
		if serverId, ok := parser.ParseString(row[0]); ok {
			return serverId != "", nil
		}
	}
	return false, nil
}

// =============================================================================
// Aurora Topology Utils
// =============================================================================

type AuroraTopologyUtils struct {
	dialect TopologyDialect
	parser  RowParser
}

func NewAuroraTopologyUtils(dialect TopologyDialect, parser RowParser) *AuroraTopologyUtils {
	return &AuroraTopologyUtils{dialect: dialect, parser: parser}
}

func (a *AuroraTopologyUtils) QueryForTopology(
	conn driver.Conn,
	initialHost, instanceTemplate *host_info_util.HostInfo,
) ([]*host_info_util.HostInfo, error) {
	rows, err := executeQuery(conn, a.dialect.GetTopologyQuery())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if len(rows.Columns()) == 0 {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("TopologyUtils.unexpectedTopologyQueryColumnCount"))
	}

	hostsMap := make(map[string]*host_info_util.HostInfo)
	row := make([]driver.Value, len(rows.Columns()))

	for rows.Next(row) == nil {
		host, err := a.createHostFromRow(row, initialHost, instanceTemplate)
		if err != nil {
			slog.Debug(error_util.GetMessage("TopologyUtils.errorProcessingQueryResults", err.Error()))
			continue
		}
		if host != nil {
			// Ensure newer records replace older ones if there are duplicate keys.
			if existing, ok := hostsMap[host.Host]; ok {
				if existing.LastUpdateTime.Before(host.LastUpdateTime) {
					hostsMap[host.Host] = host
				}
			} else {
				hostsMap[host.Host] = host
			}
		}
	}

	hosts := make([]*host_info_util.HostInfo, 0, len(hostsMap))
	for _, host := range hostsMap {
		hosts = append(hosts, host)
	}
	return verifyWriter(hosts), nil
}

// createHostFromRow: server_id (0), is_writer (1), cpu (2), lag (3), last_update_timestamp (4).
func (a *AuroraTopologyUtils) createHostFromRow(
	row []driver.Value,
	initialHost, instanceTemplate *host_info_util.HostInfo,
) (*host_info_util.HostInfo, error) {
	if len(row) < 4 {
		return nil, error_util.NewGenericAwsWrapperError("insufficient columns in topology row")
	}

	parser := a.parser
	hostName, ok := parser.ParseString(row[0])
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError("failed to parse host name")
	}

	isWriter, ok := parser.ParseBool(row[1])
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError("failed to parse is_writer")
	}

	cpu, _ := parser.ParseFloat64(row[2])
	lag, _ := parser.ParseFloat64(row[3])

	lastUpdateTime := time.Now()
	if len(row) > 4 {
		if t, ok := parser.ParseTime(row[4]); ok {
			lastUpdateTime = t
		}
	}

	weight := int(math.Round(lag)*100 + math.Round(cpu))
	host := CreateHost(hostName, hostName, isWriter, weight, lastUpdateTime, initialHost, instanceTemplate)
	if host == nil {
		return nil, error_util.NewGenericAwsWrapperError("failed to create host")
	}
	return host, nil
}

func (a *AuroraTopologyUtils) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return queryHostRole(conn, a.dialect.GetIsReaderQuery(), a.parser)
}

func (a *AuroraTopologyUtils) GetInstanceId(conn driver.Conn) (string, string) {
	return queryInstanceId(conn, a.dialect.GetInstanceIdQuery(), a.parser)
}

func (a *AuroraTopologyUtils) IsWriterInstance(conn driver.Conn) (bool, error) {
	return queryIsWriter(conn, a.dialect.GetWriterIdQuery(), a.parser, false)
}

func (a *AuroraTopologyUtils) CreateHost(
	instanceId, instanceName string, isWriter bool, weight int,
	lastUpdateTime time.Time, initialHost, instanceTemplate *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return CreateHost(instanceId, instanceName, isWriter, weight, lastUpdateTime, initialHost, instanceTemplate)
}

var _ TopologyUtils = (*AuroraTopologyUtils)(nil)

// =============================================================================
// Multi-AZ Topology Utils
// =============================================================================

type MultiAzTopologyUtils struct {
	dialect MultiAzTopologyDialect
	parser  RowParser
}

func NewMultiAzTopologyUtils(dialect MultiAzTopologyDialect, parser RowParser) *MultiAzTopologyUtils {
	return &MultiAzTopologyUtils{dialect: dialect, parser: parser}
}

func (m *MultiAzTopologyUtils) QueryForTopology(
	conn driver.Conn,
	initialHost, instanceTemplate *host_info_util.HostInfo,
) ([]*host_info_util.HostInfo, error) {
	writerId, err := m.getWriterId(conn)
	if err != nil {
		return nil, err
	}

	rows, err := executeQuery(conn, m.dialect.GetTopologyQuery())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if len(rows.Columns()) == 0 {
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("TopologyUtils.unexpectedTopologyQueryColumnCount"))
	}

	hostsMap := make(map[string]*host_info_util.HostInfo)
	row := make([]driver.Value, len(rows.Columns()))

	for rows.Next(row) == nil {
		host, err := m.createHostFromRow(row, initialHost, instanceTemplate, writerId)
		if err != nil {
			slog.Debug(error_util.GetMessage("TopologyUtils.errorProcessingQueryResults", err.Error()))
			continue
		}
		if host != nil {
			// Ensure newer records replace older ones if there are duplicate keys.
			if existing, ok := hostsMap[host.Host]; ok {
				if existing.LastUpdateTime.Before(host.LastUpdateTime) {
					hostsMap[host.Host] = host
				}
			} else {
				hostsMap[host.Host] = host
			}
		}
	}

	hosts := make([]*host_info_util.HostInfo, 0, len(hostsMap))
	for _, host := range hostsMap {
		hosts = append(hosts, host)
	}
	return verifyWriter(hosts), nil
}

func (m *MultiAzTopologyUtils) getWriterId(conn driver.Conn) (string, error) {
	writerId, err := func() (string, error) {
		rows, err := executeQuery(conn, m.dialect.GetWriterIdQuery())
		if err != nil {
			return "", err
		}
		defer rows.Close()

		row := make([]driver.Value, len(rows.Columns()))
		if rows.Next(row) == nil {
			columnName := m.dialect.GetWriterIdColumnName()
			for i, col := range rows.Columns() {
				if col == columnName {
					if id, ok := m.parser.ParseString(row[i]); ok && id != "" {
						return id, nil
					}
				}
			}
		}
		return "", nil
	}()
	if err != nil {
		return "", err
	}
	if writerId != "" {
		return writerId, nil
	}

	instanceId, _ := m.GetInstanceId(conn)
	return instanceId, nil
}

// createHostFromRow: id (0), endpoint (1), port (2).
func (m *MultiAzTopologyUtils) createHostFromRow(
	row []driver.Value,
	initialHost, instanceTemplate *host_info_util.HostInfo,
	writerId string,
) (*host_info_util.HostInfo, error) {
	if len(row) < 2 {
		return nil, error_util.NewGenericAwsWrapperError("insufficient columns in topology row")
	}

	parser := m.parser
	hostId, ok := parser.ParseString(row[0])
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError("failed to parse host id")
	}

	endpoint, ok := parser.ParseString(row[1])
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError("failed to parse endpoint")
	}

	instanceName := endpoint
	if idx := strings.Index(endpoint, "."); idx > 0 {
		instanceName = endpoint[:idx]
	}

	host := CreateHost(hostId, instanceName, hostId == writerId, 0, time.Now(), initialHost, instanceTemplate)
	if host == nil {
		return nil, error_util.NewGenericAwsWrapperError("failed to create host")
	}

	return host, nil
}

func (m *MultiAzTopologyUtils) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return queryHostRole(conn, m.dialect.GetIsReaderQuery(), m.parser)
}

func (m *MultiAzTopologyUtils) GetInstanceId(conn driver.Conn) (string, string) {
	return queryInstanceId(conn, m.dialect.GetInstanceIdQuery(), m.parser)
}

func (m *MultiAzTopologyUtils) IsWriterInstance(conn driver.Conn) (bool, error) {
	return queryIsWriter(conn, m.dialect.GetWriterIdQuery(), m.parser, true)
}

func (m *MultiAzTopologyUtils) CreateHost(
	instanceId, instanceName string, isWriter bool, weight int,
	lastUpdateTime time.Time, initialHost, instanceTemplate *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return CreateHost(instanceId, instanceName, isWriter, weight, lastUpdateTime, initialHost, instanceTemplate)
}

var _ TopologyUtils = (*MultiAzTopologyUtils)(nil)
