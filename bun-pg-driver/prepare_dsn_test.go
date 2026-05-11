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

package bun_pg_driver

import (
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/pgdriver"
)

var nilHostInfo *host_info_util.HostInfo

func newHostInfo(t *testing.T, host string, port int) *host_info_util.HostInfo {
	t.Helper()
	hi, err := host_info_util.NewHostInfoBuilder().SetHost(host).SetPort(port).Build()
	require.NoError(t, err)
	return hi
}

func TestPrepareDsn_ProducesValidPgdriverURL(t *testing.T) {
	dialect := NewBunPgDriverDialect()
	props := map[string]string{
		"user":     "myuser",
		"host":     "myhost.rds.amazonaws.com",
		"port":     "5432",
		"database": "mydb",
		"sslmode":  "require",
	}

	dsn := dialect.PrepareDsn(props, nilHostInfo)

	drv := pgdriver.NewDriver()
	connector, err := drv.OpenConnector(dsn)
	require.NoError(t, err, "pgdriver must be able to parse the DSN: %s", dsn)
	require.NotNil(t, connector)

	assert.Contains(t, dsn, "postgres://")
	assert.Contains(t, dsn, "myuser@")
	assert.Contains(t, dsn, "myhost.rds.amazonaws.com:5432")
	assert.Contains(t, dsn, "/mydb")
}

func TestPrepareDsn_HostInfoOverridesHost(t *testing.T) {
	dialect := NewBunPgDriverDialect()
	props := map[string]string{
		"user":     "myuser",
		"host":     "old-host.rds.amazonaws.com",
		"port":     "5432",
		"database": "mydb",
	}

	hostInfo := newHostInfo(t, "new-host.rds.amazonaws.com", 5433)
	dsn := dialect.PrepareDsn(props, hostInfo)

	assert.Contains(t, dsn, "new-host.rds.amazonaws.com:5433")
	assert.NotContains(t, dsn, "old-host")

	drv := pgdriver.NewDriver()
	_, err := drv.OpenConnector(dsn)
	require.NoError(t, err, "pgdriver must be able to parse the DSN with host override: %s", dsn)
}

func TestPrepareDsn_WithPassword(t *testing.T) {
	dialect := NewBunPgDriverDialect()
	props := map[string]string{
		"user":     "myuser",
		"password": "secret",
		"host":     "myhost.rds.amazonaws.com",
		"port":     "5432",
		"database": "mydb",
	}

	dsn := dialect.PrepareDsn(props, nilHostInfo)

	assert.Contains(t, dsn, "myuser:secret@")

	drv := pgdriver.NewDriver()
	_, err := drv.OpenConnector(dsn)
	require.NoError(t, err, "pgdriver must parse DSN with password: %s", dsn)
}

func TestPrepareDsn_PasswordWithSpecialCharacters(t *testing.T) {
	dialect := NewBunPgDriverDialect()
	props := map[string]string{
		"user":     "myuser",
		"password": "p@ss:wo/rd",
		"host":     "myhost.rds.amazonaws.com",
		"port":     "5432",
		"database": "mydb",
	}

	dsn := dialect.PrepareDsn(props, nilHostInfo)

	assert.Equal(t,
		"postgres://myuser:p%40ss%3Awo%2Frd@myhost.rds.amazonaws.com:5432/mydb",
		dsn)

	drv := pgdriver.NewDriver()
	_, err := drv.OpenConnector(dsn)
	require.NoError(t, err, "pgdriver must parse DSN with escaped password: %s", dsn)
}

func TestPrepareDsn_UserWithSpecialCharacters(t *testing.T) {
	dialect := NewBunPgDriverDialect()
	props := map[string]string{
		"user":     "svc:account@corp/team us-east-1",
		"password": "secret",
		"host":     "myhost.rds.amazonaws.com",
		"port":     "5432",
		"database": "mydb",
	}

	dsn := dialect.PrepareDsn(props, nilHostInfo)

	assert.Equal(t,
		"postgres://svc%3Aaccount%40corp%2Fteam%20us-east-1:secret@myhost.rds.amazonaws.com:5432/mydb",
		dsn)

	drv := pgdriver.NewDriver()
	_, err := drv.OpenConnector(dsn)
	require.NoError(t, err, "pgdriver must parse DSN with escaped user: %s", dsn)
}

func TestPrepareDsn_UserWithSpecialCharactersNoPassword(t *testing.T) {
	dialect := NewBunPgDriverDialect()
	props := map[string]string{
		"user":     "svc:account@corp/team us-east-1",
		"host":     "myhost.rds.amazonaws.com",
		"port":     "5432",
		"database": "mydb",
	}

	dsn := dialect.PrepareDsn(props, nilHostInfo)

	assert.Equal(t,
		"postgres://svc%3Aaccount%40corp%2Fteam%20us-east-1@myhost.rds.amazonaws.com:5432/mydb",
		dsn)

	drv := pgdriver.NewDriver()
	_, err := drv.OpenConnector(dsn)
	require.NoError(t, err, "pgdriver must parse DSN with escaped user, no password: %s", dsn)
}

func TestPrepareDsn_ExtraParamsAsQueryString(t *testing.T) {
	dialect := NewBunPgDriverDialect()
	props := map[string]string{
		"user":             "myuser",
		"host":             "myhost.rds.amazonaws.com",
		"port":             "5432",
		"database":         "mydb",
		"sslmode":          "require",
		"application_name": "myapp",
	}

	dsn := dialect.PrepareDsn(props, nilHostInfo)

	assert.Contains(t, dsn, "sslmode=require")
	assert.Contains(t, dsn, "application_name=myapp")

	drv := pgdriver.NewDriver()
	_, err := drv.OpenConnector(dsn)
	require.NoError(t, err, "pgdriver must parse DSN with query params: %s", dsn)
}

func TestPrepareDsn_DefaultPort(t *testing.T) {
	dialect := NewBunPgDriverDialect()
	props := map[string]string{
		"user":     "myuser",
		"host":     "myhost.rds.amazonaws.com",
		"database": "mydb",
	}

	dsn := dialect.PrepareDsn(props, nilHostInfo)

	assert.Contains(t, dsn, ":5432/")
}
