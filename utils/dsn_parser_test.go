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

	assert.Equal(t, "postgresql", props[DRIVER_PROTOCOL])
	assert.Equal(t, "someUser", props[USER])
	assert.Equal(t, "somePassword", props[PASSWORD])
	assert.Equal(t, "localhost", props[HOST])
	assert.Equal(t, "5432", props[PORT])
	assert.Equal(t, "pgx_test", props[DATABASE])
	assert.Equal(t, "disable", props["sslmode"])
	assert.Equal(t, "bar", props["foo"])
}

func TestParseDsnPgxKeyValue(t *testing.T) {
	dsn := "user=someUser password=somePassword host=localhost port=5432 database=pgx_test sslmode=disable foo=bar"
	props, err := ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "postgresql", props[DRIVER_PROTOCOL])
	assert.Equal(t, "someUser", props[USER])
	assert.Equal(t, "somePassword", props[PASSWORD])
	assert.Equal(t, "localhost", props[HOST])
	assert.Equal(t, "5432", props[PORT])
	assert.Equal(t, "pgx_test", props[DATABASE])
	assert.Equal(t, "disable", props["sslmode"])
	assert.Equal(t, "bar", props["foo"])
}

func TestParseDsnMySql(t *testing.T) {
	dsn := "someUser:somePassword@tcp(mydatabase.com:3306)/myDatabase?foo=bar&pop=snap"
	props, err := ParseDsn(dsn)

	if err != nil {
		t.Errorf(`Unexpected error when calling ParseDsn: %s, Error: %q`, dsn, err)
	}

	assert.Equal(t, "mysql", props[DRIVER_PROTOCOL])
	assert.Equal(t, "someUser", props[USER])
	assert.Equal(t, "somePassword", props[PASSWORD])
	assert.Equal(t, "mydatabase.com", props[HOST])
	assert.Equal(t, "3306", props[PORT])
	assert.Equal(t, "myDatabase", props[DATABASE])
	assert.Equal(t, "bar", props["foo"])
	assert.Equal(t, "snap", props["pop"])
}
