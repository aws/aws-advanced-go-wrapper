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

package test

import (
	"database/sql/driver"
	"testing"
	"time"

	mock_driver_infrastructure "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/awssql/driver_infrastructure"
	mock_database_sql_driver "github.com/aws/aws-advanced-go-wrapper/.test/test/mocks/database_sql_driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Helper to create a mock connection (driver.Conn + driver.QueryerContext)
// =============================================================================

func newMockConn(ctrl *gomock.Controller) (struct {
	driver.Conn
	driver.QueryerContext
}, *mock_database_sql_driver.MockQueryerContext) {
	mockConn := mock_database_sql_driver.NewMockConn(ctrl)
	mockQueryer := mock_database_sql_driver.NewMockQueryerContext(ctrl)
	conn := struct {
		driver.Conn
		driver.QueryerContext
	}{Conn: mockConn, QueryerContext: mockQueryer}
	return conn, mockQueryer
}

// =============================================================================
// AuroraTopologyUtils tests (dialect-agnostic via MockTopologyDialect)
// =============================================================================

func TestAuroraTopologyUtils_GetHostRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDialect := mock_driver_infrastructure.NewMockTopologyDialect(ctrl)
	mockParser := mock_driver_infrastructure.NewMockRowParser(ctrl)
	topologyUtils := driver_infrastructure.NewAuroraTopologyUtils(mockDialect)

	isReaderQuery := "SELECT is_reader_query"
	mockDialect.EXPECT().GetIsReaderQuery().Return(isReaderQuery).AnyTimes()
	mockDialect.EXPECT().GetRowParser().Return(mockParser).AnyTimes()

	conn, mockQueryer := newMockConn(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	// writer: ParseBool returns false (not reader)
	mockQueryer.EXPECT().QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(0)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseBool(int64(0)).Return(false, true)
	assert.Equal(t, host_info_util.WRITER, topologyUtils.GetHostRole(conn))

	// reader: ParseBool returns true (is reader)
	mockQueryer.EXPECT().QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(1)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseBool(int64(1)).Return(true, true)
	assert.Equal(t, host_info_util.READER, topologyUtils.GetHostRole(conn))

	// unknown: ParseBool fails
	mockQueryer.EXPECT().QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "unknown"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseBool("unknown").Return(false, false)
	assert.Equal(t, host_info_util.UNKNOWN, topologyUtils.GetHostRole(conn))
}

func TestAuroraTopologyUtils_QueryForTopology(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDialect := mock_driver_infrastructure.NewMockTopologyDialect(ctrl)
	mockParser := mock_driver_infrastructure.NewMockRowParser(ctrl)
	topologyUtils := driver_infrastructure.NewAuroraTopologyUtils(mockDialect)

	topologyQuery := "SELECT topology_query"
	mockDialect.EXPECT().GetTopologyQuery().Return(topologyQuery).AnyTimes()
	mockDialect.EXPECT().GetRowParser().Return(mockParser).AnyTimes()

	conn, mockQueryer := newMockConn(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	columnNames := []string{"server_id", "is_writer", "CPU", "lag", "LAST_UPDATE_TIMESTAMP"}
	now := time.Now()

	mockQueryer.EXPECT().QueryContext(gomock.Any(), topologyQuery, gomock.Any()).Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return(columnNames).AnyTimes()

	// Row 1: writer
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "host1"
		dest[1] = true
		dest[2] = 2.5
		dest[3] = 100.0
		dest[4] = now
		return nil
	})
	mockParser.EXPECT().ParseString("host1").Return("host1", true)
	mockParser.EXPECT().ParseBool(true).Return(true, true)
	mockParser.EXPECT().ParseFloat64(2.5).Return(2.5, true)
	mockParser.EXPECT().ParseFloat64(100.0).Return(100.0, true)
	mockParser.EXPECT().ParseTime(now).Return(now, true)

	// Row 2: reader
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "host2"
		dest[1] = false
		dest[2] = 3.5
		dest[3] = 50.0
		dest[4] = now
		return nil
	})
	mockParser.EXPECT().ParseString("host2").Return("host2", true)
	mockParser.EXPECT().ParseBool(false).Return(false, true)
	mockParser.EXPECT().ParseFloat64(3.5).Return(3.5, true)
	mockParser.EXPECT().ParseFloat64(50.0).Return(50.0, true)
	mockParser.EXPECT().ParseTime(now).Return(now, true)

	// No more rows
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrSkip)
	mockRows.EXPECT().Close().Return(nil)

	instanceTemplate, _ := host_info_util.NewHostInfoBuilder().SetHost("?").Build()
	hosts, err := topologyUtils.QueryForTopology(conn, nil, instanceTemplate)

	assert.NoError(t, err)
	assert.Len(t, hosts, 2)
	assert.Equal(t, host_info_util.WRITER, hosts[0].Role)
	assert.Equal(t, host_info_util.READER, hosts[1].Role)
}

func TestAuroraTopologyUtils_GetInstanceId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDialect := mock_driver_infrastructure.NewMockTopologyDialect(ctrl)
	mockParser := mock_driver_infrastructure.NewMockRowParser(ctrl)
	topologyUtils := driver_infrastructure.NewAuroraTopologyUtils(mockDialect)

	instanceIdQuery := "SELECT instance_id_query"
	mockDialect.EXPECT().GetInstanceIdQuery().Return(instanceIdQuery).AnyTimes()
	mockDialect.EXPECT().GetRowParser().Return(mockParser).AnyTimes()

	conn, mockQueryer := newMockConn(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	// Success: two columns
	mockQueryer.EXPECT().QueryContext(gomock.Any(), instanceIdQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"server_id", "server_id"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "myinstance"
		dest[1] = "myinstance"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseString("myinstance").Return("myinstance", true)
	mockParser.EXPECT().ParseString("myinstance").Return("myinstance", true)

	id, name := topologyUtils.GetInstanceId(conn)
	assert.Equal(t, "myinstance", id)
	assert.Equal(t, "myinstance", name)

	// Failure: bad conn
	mockQueryer.EXPECT().QueryContext(gomock.Any(), instanceIdQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	emptyId, emptyName := topologyUtils.GetInstanceId(conn)
	assert.Equal(t, "", emptyId)
	assert.Equal(t, "", emptyName)
}

func TestAuroraTopologyUtils_IsWriterInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDialect := mock_driver_infrastructure.NewMockTopologyDialect(ctrl)
	mockParser := mock_driver_infrastructure.NewMockRowParser(ctrl)
	topologyUtils := driver_infrastructure.NewAuroraTopologyUtils(mockDialect)

	writerIdQuery := "SELECT writer_id_query"
	mockDialect.EXPECT().GetWriterIdQuery().Return(writerIdQuery).AnyTimes()
	mockDialect.EXPECT().GetRowParser().Return(mockParser).AnyTimes()

	conn, mockQueryer := newMockConn(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	// Is writer (non-empty server_id)
	mockQueryer.EXPECT().QueryContext(gomock.Any(), writerIdQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "myinstance"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseString("myinstance").Return("myinstance", true)

	isWriter, err := topologyUtils.IsWriterInstance(conn)
	assert.NoError(t, err)
	assert.True(t, isWriter)

	// Not writer (empty server_id)
	mockQueryer.EXPECT().QueryContext(gomock.Any(), writerIdQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = ""
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseString("").Return("", true)

	isWriter, err = topologyUtils.IsWriterInstance(conn)
	assert.NoError(t, err)
	assert.False(t, isWriter)
}

// =============================================================================
// MultiAzTopologyUtils tests (dialect-agnostic via MockMultiAzTopologyDialect)
// =============================================================================

func TestMultiAzTopologyUtils_GetHostRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDialect := mock_driver_infrastructure.NewMockMultiAzTopologyDialect(ctrl)
	mockParser := mock_driver_infrastructure.NewMockRowParser(ctrl)
	topologyUtils := driver_infrastructure.NewMultiAzTopologyUtils(mockDialect)

	isReaderQuery := "SELECT is_reader_query"
	mockDialect.EXPECT().GetIsReaderQuery().Return(isReaderQuery).AnyTimes()
	mockDialect.EXPECT().GetRowParser().Return(mockParser).AnyTimes()

	conn, mockQueryer := newMockConn(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	// writer
	mockQueryer.EXPECT().QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(0)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseBool(int64(0)).Return(false, true)
	assert.Equal(t, host_info_util.WRITER, topologyUtils.GetHostRole(conn))

	// reader
	mockQueryer.EXPECT().QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(1)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseBool(int64(1)).Return(true, true)
	assert.Equal(t, host_info_util.READER, topologyUtils.GetHostRole(conn))

	// unknown
	mockQueryer.EXPECT().QueryContext(gomock.Any(), isReaderQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "unknown"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseBool("unknown").Return(false, false)
	assert.Equal(t, host_info_util.UNKNOWN, topologyUtils.GetHostRole(conn))
}

func TestMultiAzTopologyUtils_GetInstanceId(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDialect := mock_driver_infrastructure.NewMockMultiAzTopologyDialect(ctrl)
	mockParser := mock_driver_infrastructure.NewMockRowParser(ctrl)
	topologyUtils := driver_infrastructure.NewMultiAzTopologyUtils(mockDialect)

	instanceIdQuery := "SELECT instance_id_query"
	mockDialect.EXPECT().GetInstanceIdQuery().Return(instanceIdQuery).AnyTimes()
	mockDialect.EXPECT().GetRowParser().Return(mockParser).AnyTimes()

	conn, mockQueryer := newMockConn(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	// Success: two columns (id, endpoint_prefix)
	mockQueryer.EXPECT().QueryContext(gomock.Any(), instanceIdQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{"id", "endpoint_prefix"})
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "myinstance"
		dest[1] = "myinstancename"
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseString("myinstance").Return("myinstance", true)
	mockParser.EXPECT().ParseString("myinstancename").Return("myinstancename", true)

	id, name := topologyUtils.GetInstanceId(conn)
	assert.Equal(t, "myinstance", id)
	assert.Equal(t, "myinstancename", name)

	// Failure
	mockQueryer.EXPECT().QueryContext(gomock.Any(), instanceIdQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Columns().Return([]string{})
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrBadConn)
	mockRows.EXPECT().Close().Return(nil)

	emptyId, emptyName := topologyUtils.GetInstanceId(conn)
	assert.Equal(t, "", emptyId)
	assert.Equal(t, "", emptyName)
}

func TestMultiAzTopologyUtils_IsWriterInstance(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDialect := mock_driver_infrastructure.NewMockMultiAzTopologyDialect(ctrl)
	mockParser := mock_driver_infrastructure.NewMockRowParser(ctrl)
	topologyUtils := driver_infrastructure.NewMultiAzTopologyUtils(mockDialect)

	writerIdQuery := "SELECT writer_id_query"
	mockDialect.EXPECT().GetWriterIdQuery().Return(writerIdQuery).AnyTimes()
	mockDialect.EXPECT().GetRowParser().Return(mockParser).AnyTimes()

	conn, mockQueryer := newMockConn(ctrl)
	mockRows := mock_database_sql_driver.NewMockRows(ctrl)

	// Is writer (no rows returned means writer for Multi-AZ)
	mockQueryer.EXPECT().QueryContext(gomock.Any(), writerIdQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).Return(driver.ErrSkip)
	mockRows.EXPECT().Close().Return(nil)

	isWriter, err := topologyUtils.IsWriterInstance(conn)
	assert.NoError(t, err)
	assert.True(t, isWriter)

	// Not writer (has rows means it's a replica)
	mockQueryer.EXPECT().QueryContext(gomock.Any(), writerIdQuery, gomock.Nil()).Return(mockRows, nil)
	mockRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = int64(123456789)
		return nil
	})
	mockRows.EXPECT().Close().Return(nil)

	isWriter, err = topologyUtils.IsWriterInstance(conn)
	assert.NoError(t, err)
	assert.False(t, isWriter)
}

func TestMultiAzTopologyUtils_QueryForTopology(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDialect := mock_driver_infrastructure.NewMockMultiAzTopologyDialect(ctrl)
	mockParser := mock_driver_infrastructure.NewMockRowParser(ctrl)
	topologyUtils := driver_infrastructure.NewMultiAzTopologyUtils(mockDialect)

	topologyQuery := "SELECT topology_query"
	writerIdQuery := "SELECT writer_id_query"
	instanceIdQuery := "SELECT instance_id_query"
	writerIdColumnName := "Source_Server_Id"

	mockDialect.EXPECT().GetTopologyQuery().Return(topologyQuery).AnyTimes()
	mockDialect.EXPECT().GetWriterIdQuery().Return(writerIdQuery).AnyTimes()
	mockDialect.EXPECT().GetInstanceIdQuery().Return(instanceIdQuery).AnyTimes()
	mockDialect.EXPECT().GetWriterIdColumnName().Return(writerIdColumnName).AnyTimes()
	mockDialect.EXPECT().GetRowParser().Return(mockParser).AnyTimes()
	mockDialect.EXPECT().GetIsReaderQuery().Return("SELECT is_reader").AnyTimes()

	conn, mockQueryer := newMockConn(ctrl)
	mockTopologyRows := mock_database_sql_driver.NewMockRows(ctrl)
	mockHostIdRows := mock_database_sql_driver.NewMockRows(ctrl)

	expectedWriterId := "123456789"
	expectedReaderId := "132435465"
	expectedWriterEndpoint := "writerHostName.com"
	expectedReaderEndpoint := "readerHostName.com"

	// Mock topology query
	mockQueryer.EXPECT().QueryContext(gomock.Any(), topologyQuery, gomock.Nil()).Return(mockTopologyRows, nil)
	mockTopologyRows.EXPECT().Columns().Return([]string{"id", "endpoint"}).AnyTimes()
	mockTopologyRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = expectedWriterId
		dest[1] = expectedWriterEndpoint
		return nil
	})
	mockParser.EXPECT().ParseString(expectedWriterId).Return(expectedWriterId, true)
	mockParser.EXPECT().ParseString(expectedWriterEndpoint).Return(expectedWriterEndpoint, true)

	mockTopologyRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = expectedReaderId
		dest[1] = expectedReaderEndpoint
		return nil
	})
	mockParser.EXPECT().ParseString(expectedReaderId).Return(expectedReaderId, true)
	mockParser.EXPECT().ParseString(expectedReaderEndpoint).Return(expectedReaderEndpoint, true)

	mockTopologyRows.EXPECT().Next(gomock.Any()).Return(driver.ErrSkip)
	mockTopologyRows.EXPECT().Close().Return(nil)

	// Mock writer ID query (SHOW REPLICA STATUS) — returns a row with Source_Server_Id
	mockQueryer.EXPECT().QueryContext(gomock.Any(), writerIdQuery, gomock.Nil()).Return(mockHostIdRows, nil)
	mockHostIdRows.EXPECT().Columns().Return([]string{"NotId", writerIdColumnName}).AnyTimes()
	mockHostIdRows.EXPECT().Next(gomock.Any()).DoAndReturn(func(dest []driver.Value) error {
		dest[0] = "NotId"
		dest[1] = expectedWriterId
		return nil
	})
	mockHostIdRows.EXPECT().Close().Return(nil)
	mockParser.EXPECT().ParseString(expectedWriterId).Return(expectedWriterId, true)

	instanceTemplate, _ := host_info_util.NewHostInfoBuilder().SetHost("?").Build()
	hosts, err := topologyUtils.QueryForTopology(conn, nil, instanceTemplate)
	assert.NoError(t, err)
	assert.Len(t, hosts, 2)
	assert.Equal(t, host_info_util.WRITER, hosts[0].Role)
	assert.Equal(t, host_info_util.READER, hosts[1].Role)
}
