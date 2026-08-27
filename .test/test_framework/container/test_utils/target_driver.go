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

package test_utils

import (
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
)

// TargetDriver is the driver the wrapper is layered over for a test run. It is a
// separate dimension from DatabaseEngine because PostgreSQL has more than one
// supported target driver.
type TargetDriver string

const (
	PGX_DRIVER   TargetDriver = "PGX"
	BUN_PG       TargetDriver = "BUN_PG"
	MYSQL_DRIVER TargetDriver = "MYSQL"
)

const TARGET_DRIVER_ENV_VAR = "TARGET_DRIVER"

// Engine returns the database engine a target driver connects to.
func (d TargetDriver) Engine() DatabaseEngine {
	switch d {
	case PGX_DRIVER, BUN_PG:
		return PG
	case MYSQL_DRIVER:
		return MYSQL
	}
	return ""
}

// WrapperDriverCode returns the name the target driver's wrapper is registered
// under with database/sql.
func (d TargetDriver) WrapperDriverCode() string {
	switch d {
	case PGX_DRIVER:
		return driver_infrastructure.AWS_PGX_DRIVER_CODE
	case BUN_PG:
		return driver_infrastructure.AWS_BUN_PG_DRIVER_CODE
	case MYSQL_DRIVER:
		return driver_infrastructure.AWS_MYSQL_DRIVER_CODE
	}
	return ""
}

// SupportsNamedArgs reports whether the target driver accepts named query
// arguments. Only pgx does; bun's pgdriver interpolates positional $N
// placeholders client-side and has no named-argument syntax.
func (d TargetDriver) SupportsNamedArgs() bool {
	return d == PGX_DRIVER
}

// DefaultDsnProps returns DSN parameters a driver needs before the suite's own
// queries will work, for callers to apply where the caller has not set them.
//
// Only bun needs any. pgdriver applies a 10 second read and 5 second write socket
// deadline by default and takes the earlier of its own deadline and the context's,
// so without these every sleep query in the suite dies on the socket rather than
// on the behaviour under test. The wrapper cannot supply them: its property
// resolver feeds getMonitoringProps, which builds topology monitoring connections
// only, so the main connection is left with pgdriver's defaults.
//
// The value must exceed the longest context deadline the suite sets, 61 seconds,
// so that the context stays the effective bound. It must not be zero or negative:
// pgdriver maps those to an already-expired deadline rather than to "no timeout".
func (d TargetDriver) DefaultDsnProps() map[string]string {
	if d != BUN_PG {
		return nil
	}
	return map[string]string{
		"read_timeout":  "180",
		"write_timeout": "180",
	}
}

// defaultTargetDriverForEngine returns the driver used when a run does not ask
// for a specific one.
func defaultTargetDriverForEngine(engine DatabaseEngine) (TargetDriver, error) {
	switch engine {
	case PG:
		return PGX_DRIVER, nil
	case MYSQL:
		return MYSQL_DRIVER, nil
	}
	return "", fmt.Errorf("cannot pick a default target driver for engine %q", engine)
}

// ResolveTargetDriver turns the raw target driver from the test environment into
// a TargetDriver, defaulting per engine when it is absent.
//
// An unrecognised value, or one whose engine does not match, is an error rather
// than a fall back to the default: a run that silently used pgx while reporting
// itself as some other driver would be worse than a run that failed outright.
func ResolveTargetDriver(raw string, engine DatabaseEngine) (TargetDriver, error) {
	if raw == "" {
		return defaultTargetDriverForEngine(engine)
	}

	driver := TargetDriver(raw)
	switch driver {
	case PGX_DRIVER, BUN_PG, MYSQL_DRIVER:
	default:
		return "", fmt.Errorf("unknown target driver %q", raw)
	}

	if driver.Engine() != engine {
		return "", fmt.Errorf("target driver %q cannot be used with engine %q, it requires %q", driver, engine, driver.Engine())
	}
	return driver, nil
}

// TargetDriverForEngine returns the target driver this run is exercising, checked
// against the engine the caller expects.
func TargetDriverForEngine(engine DatabaseEngine) (TargetDriver, error) {
	return ResolveTargetDriver(os.Getenv(TARGET_DRIVER_ENV_VAR), engine)
}

// MustTargetDriverForEngine is TargetDriverForEngine for callers that cannot
// report an error. A test environment that cannot name its target driver is a
// broken run, not a run to continue on a guess.
func MustTargetDriverForEngine(engine DatabaseEngine) TargetDriver {
	driver, err := TargetDriverForEngine(engine)
	if err != nil {
		panic(fmt.Sprintf("unable to determine the target driver: %s", err.Error()))
	}
	return driver
}

// SkipForTargetDrivers skips a test on the given target drivers. The reason is
// required, so a skipped test says why it is skipped and what would un-skip it.
func SkipForTargetDrivers(t *testing.T, reason string, driversToSkip ...TargetDriver) {
	environment, err := GetCurrentTestEnvironment()
	if err != nil {
		return
	}

	current := MustTargetDriverForEngine(environment.Info().Request.Engine)
	for _, driverToSkip := range driversToSkip {
		if current == driverToSkip {
			t.Skipf("Skipping test for target driver %s: %s", current, reason)
			return
		}
	}
}
