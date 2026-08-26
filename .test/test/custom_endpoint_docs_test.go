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
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// customEndpointDocPath is the user guide for the plugin. go test runs with the working directory
// set to the package directory (.test/test), so the repository root is two levels up.
const customEndpointDocPath = "../../docs/user-guide/using-plugins/UsingTheCustomEndpointPlugin.md"

// customEndpointProperties are the connection parameters the plugin actually reads.
func customEndpointProperties() []property_util.AwsWrapperProperty {
	return []property_util.AwsWrapperProperty{
		property_util.CUSTOM_ENDPOINT_REGION_PROPERTY,
		property_util.CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS,
		property_util.CUSTOM_ENDPOINT_INFO_REFRESH_RATE_BACKOFF_FACTOR,
		property_util.CUSTOM_ENDPOINT_INFO_MAX_REFRESH_RATE_MS,
		property_util.CUSTOM_ENDPOINT_ENFORCE_ROLE_FILTERING,
		property_util.CUSTOM_ENDPOINT_MONITOR_IDLE_EXPIRATION_MS,
		property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO,
		property_util.WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS,
	}
}

func readCustomEndpointDoc(t *testing.T) string {
	t.Helper()
	content, err := os.ReadFile(filepath.FromSlash(customEndpointDocPath))
	require.NoError(t, err, "could not read %s", customEndpointDocPath)
	return string(content)
}

// TestCustomEndpointDocDocumentsEveryProperty catches a documented parameter drifting away from the
// code, in both directions. The guide carried `wrapperPlugins`, which is not a property this
// wrapper has, for months, and shipped two real properties undocumented, because nothing compared
// the two.
//
// Undocumented properties are the more damaging direction: unrecognised keys are not rejected, they
// are forwarded into the target driver's DSN, so a misspelling surfaces as an opaque
// "unrecognized configuration parameter" from the database rather than as a wrapper error.
func TestCustomEndpointDocDocumentsEveryProperty(t *testing.T) {
	doc := readCustomEndpointDoc(t)

	for _, property := range customEndpointProperties() {
		assert.Containsf(t, doc, "`"+property.Name+"`",
			"property %q is read by the customEndpoint plugin but is not documented in %s",
			property.Name, customEndpointDocPath)
	}
}

// TestCustomEndpointDocHasNoUnknownParameters asserts the reverse: every backticked identifier in
// the doc that looks like a connection parameter is a property this wrapper really has.
func TestCustomEndpointDocHasNoUnknownParameters(t *testing.T) {
	doc := readCustomEndpointDoc(t)

	// Candidates are backticked lowerCamelCase tokens with at least one internal capital, which is
	// the shape of every wrapper property name. Requiring the hump keeps literals out of the
	// results: the parameter table backticks its default values too, so `true` would otherwise be
	// reported as an unknown parameter.
	candidatePattern := regexp.MustCompile("`([a-z][a-zA-Z0-9]*[A-Z][a-zA-Z0-9]*)`")

	// Identifiers matching that shape that are legitimately mentioned but are not parameters.
	notProperties := map[string]bool{
		"auroraConnectionTracker":         true, // a plugin code
		"auroraInitialConnectionStrategy": true, // a plugin code
		"customEndpoint":                  true, // a plugin code
		"strictReader":                    true, // a failoverMode value
		"setReadOnly":                     true, // a driver method
	}

	var unknown []string
	for _, match := range candidatePattern.FindAllStringSubmatch(doc, -1) {
		name := match[1]
		if notProperties[name] || property_util.ALL_WRAPPER_PROPERTIES[name] {
			continue
		}
		unknown = append(unknown, name)
	}
	sort.Strings(unknown)

	assert.Emptyf(t, unknown,
		"%s references %v, which are not properties in ALL_WRAPPER_PROPERTIES. "+
			"A parameter name that does not exist is silently forwarded into the target driver DSN, "+
			"so following the doc produces an opaque database error. Correct the doc, or add the "+
			"identifier to notProperties if it is not a connection parameter.",
		customEndpointDocPath, unknown)
}

// TestCustomEndpointDocDocumentsRequiredIamPermission guards the one line most likely to cause an
// outage. The permission is required regardless of database authentication method; the guide
// previously scoped it to "when using IAM authentication", so a password-auth reader would
// reasonably conclude it did not apply to them.
func TestCustomEndpointDocDocumentsRequiredIamPermission(t *testing.T) {
	doc := readCustomEndpointDoc(t)

	assert.Contains(t, doc, "rds:DescribeDBClusterEndpoints",
		"the guide must name the IAM action the plugin calls")
	assert.Contains(t, doc, "regardless of how your application authenticates",
		"the guide must state that the permission is not conditional on IAM database authentication")
}

// documentedDefault returns the Default column of the parameter table row for a property, reduced to
// its first backticked token. The column may carry a gloss after the value, as
// "`900000` (15 minutes)" does.
func documentedDefault(t *testing.T, doc string, name string) (string, bool) {
	t.Helper()
	row := regexp.MustCompile("(?m)^\\|\\s*`" + regexp.QuoteMeta(name) + "`\\s*\\|.*$").FindString(doc)
	if row == "" {
		return "", false
	}
	// Leading "|" makes cells[0] empty, so the columns are 1-indexed: parameter, value, required,
	// description, default.
	cells := strings.Split(row, "|")
	if len(cells) < 6 {
		return "", false
	}
	value := regexp.MustCompile("`([^`]*)`").FindStringSubmatch(cells[5])
	if value == nil {
		return "", false
	}
	return value[1], true
}

// TestCustomEndpointDocDocumentsCurrentDefaults compares the Default column of the parameter table
// against the values the code actually ships.
//
// The name checks above pass whatever the table claims a default is, so a default could be changed in
// property_util and left stale in the guide with nothing failing. That is a costly kind of stale: a
// reader tunes against a number that is not the number in effect, and the guide is the only place most
// consumers will look.
func TestCustomEndpointDocDocumentsCurrentDefaults(t *testing.T) {
	doc := readCustomEndpointDoc(t)

	for _, property := range customEndpointProperties() {
		documented, found := documentedDefault(t, doc, property.Name)
		if !assert.Truef(t, found,
			"no default could be read from the %q row of the parameter table in %s",
			property.Name, customEndpointDocPath) {
			continue
		}
		// Get with no properties returns the raw default, without the type conversion that
		// GetVerifiedWrapperPropertyValue applies - the table documents the literal.
		expected := property.Get(nil)
		// An empty default is documented as an empty string literal rather than as a blank cell.
		if expected == "" {
			expected = `""`
		}
		assert.Equalf(t, expected, documented,
			"%s documents a default of %q for %q, but the code ships %q",
			customEndpointDocPath, documented, property.Name, expected)
	}
}
