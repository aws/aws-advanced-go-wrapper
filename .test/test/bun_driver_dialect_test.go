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
	"errors"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	bun_driver "github.com/aws/aws-advanced-go-wrapper/bun-driver"
	"github.com/stretchr/testify/assert"
)

func TestBunPrepareDsn(t *testing.T) {
	driverDialect := &bun_driver.BunPgDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PASSWORD.Name: "password",
		property_util.PORT.Name:     "5432",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		property_util.PLUGINS.Name:  "test",
		"monitoring-user":           "monitor-user",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)

	assert.Contains(t, dsn, "postgres://")
	assert.Contains(t, dsn, "user:password@")
	assert.Contains(t, dsn, "host:5432")
	assert.Contains(t, dsn, "/dbName")
	assert.NotContains(t, dsn, "plugins")
	assert.NotContains(t, dsn, "monitor-user")
}

func TestBunPrepareDsnWithoutPassword(t *testing.T) {
	driverDialect := &bun_driver.BunPgDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PORT.Name:     "5432",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)

	assert.Contains(t, dsn, "postgres://user@host:5432/dbName")
	assert.NotContains(t, dsn, "user:@")
}

func TestBunPrepareDsnDefaultPort(t *testing.T) {
	driverDialect := &bun_driver.BunPgDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)

	assert.Contains(t, dsn, "host:5432/")
}

func TestBunPrepareDsnExtraParams(t *testing.T) {
	driverDialect := &bun_driver.BunPgDriverDialect{}

	properties := map[string]string{
		property_util.USER.Name:     "user",
		property_util.PORT.Name:     "5432",
		property_util.HOST.Name:     "host",
		property_util.DATABASE.Name: "dbName",
		"sslmode":                   "require",
		"application_name":          "myapp",
	}

	dsn := driverDialect.PrepareDsn(properties, nil)

	assert.Contains(t, dsn, "sslmode=require")
	assert.Contains(t, dsn, "application_name=myapp")
}

func TestBunErrorHandler(t *testing.T) {
	errorHandler := &bun_driver.BunPgErrorHandler{}

	// pgdriver.Error has unexported fields so we can't construct one with a
	// specific SQLSTATE. Test the message-based fallback paths instead.
	for _, message := range bun_driver.PgNetworkErrorMessages {
		err := errors.New(message)
		assert.True(t, errorHandler.IsNetworkError(err), "expected network error for: %s", message)
		assert.False(t, errorHandler.IsLoginError(err), "should not be login error for: %s", message)
	}

	for _, code := range bun_driver.AccessErrors {
		err := errors.New("authentication failed " + code)
		assert.True(t, errorHandler.IsLoginError(err), "expected login error for code in message: %s", code)
	}

	assert.False(t, errorHandler.IsNetworkError(errors.New("unique constraint violation")))
	assert.False(t, errorHandler.IsLoginError(errors.New("unique constraint violation")))
}
