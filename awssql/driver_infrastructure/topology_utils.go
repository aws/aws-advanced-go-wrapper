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
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// =============================================================================
// Interface
// =============================================================================

// TopologyUtils defines the base interface for common topology operations
// shared by all topology types (Aurora, Multi-AZ, Global Aurora).
type TopologyUtils interface {
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

// ClusterTopologyUtils extends TopologyUtils with single-cluster topology querying
// for Aurora and Multi-AZ clusters.
type ClusterTopologyUtils interface {
	TopologyUtils
	QueryForTopology(conn driver.Conn, initialHost *host_info_util.HostInfo, instanceTemplate *host_info_util.HostInfo) ([]*host_info_util.HostInfo, error)
}

// =============================================================================
// Shared Helper Functions
// =============================================================================

// If queryTimeoutMs is 0, then we will be using the socket timeout from DSN.
func executeQuery(conn driver.Conn, query string, queryTimeoutMs int) (driver.Rows, error) {
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
	}

	ctx := context.Background()
	if queryTimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(queryTimeoutMs)*time.Millisecond)
		defer cancel()
	}
	return queryerCtx.QueryContext(ctx, query, nil)
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

func resolveQueryTimeoutMs(driverDialect DriverDialect, props *utils.RWMap[string, string]) int {
	if SupportsSocketTimeoutViaDsn(driverDialect) {
		return 0
	}
	return property_util.GetVerifiedWrapperPropertyValue[int](props, property_util.CLUSTER_TOPOLOGY_SOCKET_TIMEOUT_MS)
}

// =============================================================================
// Shared topology query helpers
// =============================================================================

// queryHostRole executes the is-reader query and returns the host role.
func queryHostRole(conn driver.Conn, query string, parser RowParser, queryTimeoutMs int) host_info_util.HostRole {
	rows, err := executeQuery(conn, query, queryTimeoutMs)
	if err != nil {
		return host_info_util.UNKNOWN
	}
	defer func() { _ = rows.Close() }()

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
func queryInstanceId(conn driver.Conn, query string, parser RowParser, queryTimeoutMs int) (string, string) {
	rows, err := executeQuery(conn, query, queryTimeoutMs)
	if err != nil {
		return "", ""
	}
	defer func() { _ = rows.Close() }()

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
func queryIsWriter(conn driver.Conn, query string, parser RowParser, writerWhenEmpty bool, queryTimeoutMs int) (bool, error) {
	rows, err := executeQuery(conn, query, queryTimeoutMs)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

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
	dialect        TopologyDialect
	parser         RowParser
	queryTimeoutMs int
}

func NewAuroraTopologyUtils(dialect TopologyDialect, driverDialect DriverDialect, props *utils.RWMap[string, string]) *AuroraTopologyUtils {
	return &AuroraTopologyUtils{
		dialect:        dialect,
		parser:         driverDialect.GetRowParser(),
		queryTimeoutMs: resolveQueryTimeoutMs(driverDialect, props),
	}
}

func (a *AuroraTopologyUtils) QueryForTopology(
	conn driver.Conn,
	initialHost, instanceTemplate *host_info_util.HostInfo,
) ([]*host_info_util.HostInfo, error) {
	rows, err := executeQuery(conn, a.dialect.GetTopologyQuery(), a.queryTimeoutMs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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
	return queryHostRole(conn, a.dialect.GetIsReaderQuery(), a.parser, a.queryTimeoutMs)
}

func (a *AuroraTopologyUtils) GetInstanceId(conn driver.Conn) (string, string) {
	return queryInstanceId(conn, a.dialect.GetInstanceIdQuery(), a.parser, a.queryTimeoutMs)
}

func (a *AuroraTopologyUtils) IsWriterInstance(conn driver.Conn) (bool, error) {
	return queryIsWriter(conn, a.dialect.GetWriterIdQuery(), a.parser, false, a.queryTimeoutMs)
}

func (a *AuroraTopologyUtils) CreateHost(
	instanceId, instanceName string, isWriter bool, weight int,
	lastUpdateTime time.Time, initialHost, instanceTemplate *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return CreateHost(instanceId, instanceName, isWriter, weight, lastUpdateTime, initialHost, instanceTemplate)
}

var _ ClusterTopologyUtils = (*AuroraTopologyUtils)(nil)

// =============================================================================
// Multi-AZ Topology Utils
// =============================================================================

type MultiAzTopologyUtils struct {
	dialect        MultiAzTopologyDialect
	parser         RowParser
	queryTimeoutMs int
}

func NewMultiAzTopologyUtils(dialect MultiAzTopologyDialect, driverDialect DriverDialect, props *utils.RWMap[string, string]) *MultiAzTopologyUtils {
	return &MultiAzTopologyUtils{
		dialect:        dialect,
		parser:         driverDialect.GetRowParser(),
		queryTimeoutMs: resolveQueryTimeoutMs(driverDialect, props),
	}
}

func (m *MultiAzTopologyUtils) QueryForTopology(
	conn driver.Conn,
	initialHost, instanceTemplate *host_info_util.HostInfo,
) ([]*host_info_util.HostInfo, error) {
	writerId, err := m.getWriterId(conn)
	if err != nil {
		return nil, err
	}

	rows, err := executeQuery(conn, m.dialect.GetTopologyQuery(), m.queryTimeoutMs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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
		rows, err := executeQuery(conn, m.dialect.GetWriterIdQuery(), m.queryTimeoutMs)
		if err != nil {
			return "", err
		}
		defer func() { _ = rows.Close() }()

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
	return queryHostRole(conn, m.dialect.GetIsReaderQuery(), m.parser, m.queryTimeoutMs)
}

func (m *MultiAzTopologyUtils) GetInstanceId(conn driver.Conn) (string, string) {
	return queryInstanceId(conn, m.dialect.GetInstanceIdQuery(), m.parser, m.queryTimeoutMs)
}

func (m *MultiAzTopologyUtils) IsWriterInstance(conn driver.Conn) (bool, error) {
	return queryIsWriter(conn, m.dialect.GetWriterIdQuery(), m.parser, true, m.queryTimeoutMs)
}

func (m *MultiAzTopologyUtils) CreateHost(
	instanceId, instanceName string, isWriter bool, weight int,
	lastUpdateTime time.Time, initialHost, instanceTemplate *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return CreateHost(instanceId, instanceName, isWriter, weight, lastUpdateTime, initialHost, instanceTemplate)
}

var _ ClusterTopologyUtils = (*MultiAzTopologyUtils)(nil)

// =============================================================================
// Global Aurora Topology Utils
// =============================================================================

// GlobalClusterTopologyUtils extends TopologyUtils with multi-region support for
// Global Aurora Databases. It provides region-aware topology queries and region
// lookups needed by the global topology query strategy.
type GlobalClusterTopologyUtils interface {
	TopologyUtils
	// QueryForTopologyByRegion fetches the current cluster topology using region-specific
	// instance templates to resolve host endpoints for each region in the global database.
	QueryForTopologyByRegion(
		conn driver.Conn,
		initialHost *host_info_util.HostInfo,
		instanceTemplatesByRegion map[string]*host_info_util.HostInfo,
	) ([]*host_info_util.HostInfo, error)
	// GetRegion queries the database to get the AWS region for a given instance ID.
	GetRegion(instanceId string, conn driver.Conn) (string, error)
}

type GlobalAuroraTopologyUtils struct {
	dialect        GlobalAuroraTopologyDialect
	parser         RowParser
	queryTimeoutMs int
}

func NewGlobalAuroraTopologyUtils(dialect GlobalAuroraTopologyDialect, driverDialect DriverDialect, props *utils.RWMap[string, string]) *GlobalAuroraTopologyUtils {
	return &GlobalAuroraTopologyUtils{
		dialect:        dialect,
		parser:         driverDialect.GetRowParser(),
		queryTimeoutMs: resolveQueryTimeoutMs(driverDialect, props),
	}
}

func (g *GlobalAuroraTopologyUtils) QueryForTopologyByRegion(
	conn driver.Conn,
	initialHost *host_info_util.HostInfo,
	instanceTemplatesByRegion map[string]*host_info_util.HostInfo,
) ([]*host_info_util.HostInfo, error) {
	rows, err := executeQuery(conn, g.dialect.GetTopologyQuery(), g.queryTimeoutMs)
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
		host, err := g.createHostFromRow(row, initialHost, instanceTemplatesByRegion)
		if err != nil {
			slog.Debug(error_util.GetMessage("TopologyUtils.errorProcessingQueryResults", err.Error()))
			continue
		}
		if host != nil {
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

// createHostFromRow: server_id (0), is_writer (1), visibility_lag_in_msec (2), aws_region (3).
func (g *GlobalAuroraTopologyUtils) createHostFromRow(
	row []driver.Value,
	initialHost *host_info_util.HostInfo,
	instanceTemplatesByRegion map[string]*host_info_util.HostInfo,
) (*host_info_util.HostInfo, error) {
	if len(row) < 4 {
		return nil, error_util.NewGenericAwsWrapperError("insufficient columns in global topology row")
	}

	parser := g.parser
	hostName, ok := parser.ParseString(row[0])
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError("failed to parse host name")
	}

	isWriter, ok := parser.ParseBool(row[1])
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError("failed to parse is_writer")
	}

	nodeLag, _ := parser.ParseFloat64(row[2])
	weight := int(math.Round(nodeLag) * 100)

	awsRegion, ok := parser.ParseString(row[3])
	if !ok {
		return nil, error_util.NewGenericAwsWrapperError("failed to parse aws_region")
	}

	regionTemplate, found := instanceTemplatesByRegion[awsRegion]
	if !found {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("GlobalAuroraTopologyMonitor.cannotFindRegionTemplate", awsRegion))
	}

	host := CreateHost(hostName, hostName, isWriter, weight, time.Now(), initialHost, regionTemplate)
	if host == nil {
		return nil, error_util.NewGenericAwsWrapperError("failed to create host")
	}
	return host, nil
}

func (g *GlobalAuroraTopologyUtils) GetHostRole(conn driver.Conn) host_info_util.HostRole {
	return queryHostRole(conn, g.dialect.GetIsReaderQuery(), g.parser, g.queryTimeoutMs)
}

func (g *GlobalAuroraTopologyUtils) GetInstanceId(conn driver.Conn) (string, string) {
	return queryInstanceId(conn, g.dialect.GetInstanceIdQuery(), g.parser, g.queryTimeoutMs)
}

func (g *GlobalAuroraTopologyUtils) IsWriterInstance(conn driver.Conn) (bool, error) {
	return queryIsWriter(conn, g.dialect.GetWriterIdQuery(), g.parser, false, g.queryTimeoutMs)
}

func (g *GlobalAuroraTopologyUtils) CreateHost(
	instanceId, instanceName string, isWriter bool, weight int,
	lastUpdateTime time.Time, initialHost, instanceTemplate *host_info_util.HostInfo,
) *host_info_util.HostInfo {
	return CreateHost(instanceId, instanceName, isWriter, weight, lastUpdateTime, initialHost, instanceTemplate)
}

// GetRegion queries the database to get the AWS region for a given instance ID.
func (g *GlobalAuroraTopologyUtils) GetRegion(instanceId string, conn driver.Conn) (string, error) {
	queryerCtx, ok := conn.(driver.QueryerContext)
	if !ok {
		return "", error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("Conn.doesNotImplementRequiredInterface", "driver.QueryerContext"))
	}

	rows, err := queryerCtx.QueryContext(context.Background(), g.dialect.GetRegionByInstanceIdQuery(), []driver.NamedValue{
		{Ordinal: 1, Value: instanceId},
	})
	if err != nil {
		return "", err
	}
	defer rows.Close()

	row := make([]driver.Value, 1)
	if rows.Next(row) == nil {
		if region, ok := g.parser.ParseString(row[0]); ok && region != "" {
			return region, nil
		}
	}
	return "", nil
}

// ParseInstanceTemplates parses a comma-separated string of "[region]host:port" patterns
// into a map of region to HostInfo templates.
func ParseInstanceTemplates(instanceTemplatesString string, defaultPort int) (map[string]*host_info_util.HostInfo, error) {
	if instanceTemplatesString == "" {
		return nil, error_util.NewGenericAwsWrapperError(
			error_util.GetMessage("GlobalAuroraTopologyUtils.globalClusterInstanceHostPatternsRequired"))
	}

	result := make(map[string]*host_info_util.HostInfo)
	entries := strings.Split(instanceTemplatesString, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		region, hostInfo, err := parseHostPortPairWithRegionPrefix(entry, defaultPort)
		if err != nil {
			return nil, err
		}
		result[region] = hostInfo
	}
	return result, nil
}

var urlWithRegionPattern = regexp.MustCompile(`^(?:\[(?P<region>.+)\])?(?P<domain>[a-zA-Z0-9?\.\-]+)(?::(?P<port>[0-9]+))?$`)

func parseHostPortPairWithRegionPrefix(urlWithRegionPrefix string, defaultPort int) (string, *host_info_util.HostInfo, error) {
	matches := urlWithRegionPattern.FindStringSubmatch(urlWithRegionPrefix)
	if matches == nil {
		return "", nil, error_util.NewGenericAwsWrapperError("cannot parse URL: " + urlWithRegionPrefix)
	}

	regionIdx := urlWithRegionPattern.SubexpIndex("region")
	domainIdx := urlWithRegionPattern.SubexpIndex("domain")
	portIdx := urlWithRegionPattern.SubexpIndex("port")

	awsRegion := ""
	if regionIdx >= 0 && regionIdx < len(matches) {
		awsRegion = matches[regionIdx]
	}

	host := ""
	if domainIdx >= 0 && domainIdx < len(matches) {
		host = matches[domainIdx]
	}
	if host == "" {
		return "", nil, error_util.NewGenericAwsWrapperError("cannot parse host from: " + urlWithRegionPrefix)
	}

	if awsRegion == "" {
		awsRegion = utils.GetRdsRegion(host)
		if awsRegion == "" {
			return "", nil, error_util.NewGenericAwsWrapperError("cannot parse AWS region from: " + urlWithRegionPrefix)
		}
	}

	port := defaultPort
	if portIdx >= 0 && portIdx < len(matches) && matches[portIdx] != "" {
		fmt.Sscanf(matches[portIdx], "%d", &port)
	}

	hostInfo, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	if err != nil {
		return "", nil, err
	}
	return awsRegion, hostInfo, nil
}

var _ GlobalClusterTopologyUtils = (*GlobalAuroraTopologyUtils)(nil)
