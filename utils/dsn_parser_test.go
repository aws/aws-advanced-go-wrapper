package utils

import (
	"awssql/driver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetHostsFromDsnWithPgxDsnUrl(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar"
	hosts, err := GetHostsFromDsn(dsn, true)

	if err != nil {
		t.Errorf(`Unexpected error when calling GetHostsFromDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, 1, len(hosts))
	assert.Equal(t, "localhost", hosts[0].Host)
	assert.Equal(t, 5432, hosts[0].Port)
	assert.Equal(t, driver.AVAILABLE, hosts[0].Availability)
	assert.Equal(t, driver.WRITER, hosts[0].Role)
	assert.Equal(t, driver.HOST_DEFAULT_WEIGHT, hosts[0].Weight)
}

func TestParseDsnPgxUrl(t *testing.T) {
	dsn := "postgres://someUser:somePassword@localhost:5432/pgx_test?sslmode=disable&foo=bar"
	props, err := ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, props[PROTOCOL], "postgres")
	assert.Equal(t, props[USER], "someUser")
	assert.Equal(t, props[PASSWORD], "somePassword")
	assert.Equal(t, props[HOST], "localhost")
	assert.Equal(t, props[PORT], "5432")
	assert.Equal(t, props[DATABASE], "pgx_test")
	assert.Equal(t, props["sslmode"], "disable")
	assert.Equal(t, props["foo"], "bar")
}

func TestParseDsnPgxKeyValue(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=disable foo=bar"
	props, err := ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, props[PROTOCOL], "postgresql")
	assert.Equal(t, props[USER], "someUser")
	assert.Equal(t, props[PASSWORD], "somePassword")
	assert.Equal(t, props[HOST], "localhost")
	assert.Equal(t, props[PORT], "5432")
	assert.Equal(t, props[DATABASE], "pgx_test")
	assert.Equal(t, props["sslmode"], "disable")
	assert.Equal(t, props["foo"], "bar")
}

func TestParseDsnMySql(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar"
	props, err := ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, props[PROTOCOL], "mysql")
	assert.Equal(t, props[USER], "someUser")
	assert.Equal(t, props[PASSWORD], "somePassword")
	assert.Equal(t, props[HOST], "mydatabase.com")
	assert.Equal(t, props[PORT], "3306")
	assert.Equal(t, props[DATABASE], "myDatabase")
	assert.Equal(t, props["foo"], "bar")
}
