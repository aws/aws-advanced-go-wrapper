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
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hostWithRole builds a host whose HostId is its name, which is what the allow/block lists key on.
func hostWithRole(t *testing.T, id string, role host_info_util.HostRole) *host_info_util.HostInfo {
	t.Helper()
	host, err := host_info_util.NewHostInfoBuilder().
		SetHost(id + ".cluster-custom-xyz.us-east-2.rds.amazonaws.com").
		SetPort(5432).SetHostId(id).SetRole(role).Build()
	require.NoError(t, err)
	return host
}

func hostIds(hosts []*host_info_util.HostInfo) []string {
	ids := make([]string, 0, len(hosts))
	for _, host := range hosts {
		ids = append(ids, host.HostId)
	}
	return ids
}

func TestAllowedAndBlockedHosts_FilterHosts(t *testing.T) {
	writer := hostWithRole(t, "writer-1", host_info_util.WRITER)
	reader1 := hostWithRole(t, "reader-1", host_info_util.READER)
	reader2 := hostWithRole(t, "reader-2", host_info_util.READER)
	all := []*host_info_util.HostInfo{writer, reader1, reader2}

	tests := []struct {
		name     string
		allowed  map[string]bool
		blocked  map[string]bool
		expected []string
	}{
		{
			name:     "no restrictions returns every host",
			expected: []string{"writer-1", "reader-1", "reader-2"},
		},
		{
			// A static member list routes to every listed member, a writer included, so a writer that
			// is a static member stays eligible.
			name:     "static member list keeps a listed writer",
			allowed:  map[string]bool{"writer-1": true, "reader-1": true},
			expected: []string{"writer-1", "reader-1"},
		},
		{
			name:     "exclusion list drops the excluded host",
			blocked:  map[string]bool{"reader-2": true},
			expected: []string{"writer-1", "reader-1"},
		},
		{
			// The shape the read/write splitting and reader failover regressions both stem from: an
			// exclusion list that names the writer leaves a host list with no writer in it at all.
			name:     "excluding the writer leaves a writer-less list",
			blocked:  map[string]bool{"writer-1": true},
			expected: []string{"reader-1", "reader-2"},
		},
		{
			// And the narrower shape: exactly one host, which is a reader rather than the writer.
			name:     "a single allowed reader is the only survivor",
			allowed:  map[string]bool{"reader-1": true},
			expected: []string{"reader-1"},
		},
		{
			name:     "an allow list naming an absent host empties the result",
			allowed:  map[string]bool{"not-in-topology": true},
			expected: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			permissions := driver_infrastructure.NewAllowedAndBlockedHosts(test.allowed, test.blocked)
			assert.Equal(t, test.expected, hostIds(permissions.FilterHosts(all)))
		})
	}
}

// TestAllowedAndBlockedHosts_RoleRequirement covers the role filter. The requirement has to hold even
// when neither id list restricts anything, which is what a READER endpoint excluding nothing produces.
func TestAllowedAndBlockedHosts_RoleRequirement(t *testing.T) {
	writer := hostWithRole(t, "writer-1", host_info_util.WRITER)
	reader1 := hostWithRole(t, "reader-1", host_info_util.READER)
	reader2 := hostWithRole(t, "reader-2", host_info_util.READER)
	unknown := hostWithRole(t, "unknown-1", host_info_util.UNKNOWN)
	all := []*host_info_util.HostInfo{writer, reader1, reader2, unknown}

	tests := []struct {
		name     string
		blocked  map[string]bool
		role     host_info_util.HostRole
		expected []string
	}{
		{"no requirement keeps every host", nil, host_info_util.UNKNOWN,
			[]string{"writer-1", "reader-1", "reader-2", "unknown-1"}},
		{"reader requirement drops the writer", nil, host_info_util.READER,
			[]string{"reader-1", "reader-2"}},
		{"reader requirement applies with an empty exclusion list", nil, host_info_util.READER,
			[]string{"reader-1", "reader-2"}},
		{"an unknown role does not satisfy a reader requirement",
			map[string]bool{"reader-1": true, "reader-2": true}, host_info_util.READER, []string{}},
		{"writer requirement keeps only the writer", nil, host_info_util.WRITER, []string{"writer-1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			permissions := driver_infrastructure.NewAllowedAndBlockedHostsWithRole(nil, test.blocked, test.role)
			assert.Equal(t, test.expected, hostIds(permissions.FilterHosts(all)))
			assert.Equal(t, test.role, permissions.GetRequiredRole())
		})
	}
}

// TestAllowedAndBlockedHosts_DeprecatedConstructorHasNoRole pins the compatibility guarantee: the
// two-argument form still compiles and must mean "no role requirement", not the zero value of HostRole,
// which is "" and would filter every host away.
func TestAllowedAndBlockedHosts_DeprecatedConstructorHasNoRole(t *testing.T) {
	permissions := driver_infrastructure.NewAllowedAndBlockedHosts(nil, nil)
	all := []*host_info_util.HostInfo{
		hostWithRole(t, "writer-1", host_info_util.WRITER),
		hostWithRole(t, "reader-1", host_info_util.READER),
	}
	assert.Equal(t, host_info_util.UNKNOWN, permissions.GetRequiredRole())
	assert.Equal(t, []string{"writer-1", "reader-1"}, hostIds(permissions.FilterHosts(all)))
}

// TestAllowedAndBlockedHosts_FilterHostsNilReceiver covers the absent-permissions path: GetHosts
// calls this before any monitor has published, and it must fail open rather than panic.
func TestAllowedAndBlockedHosts_FilterHostsNilReceiver(t *testing.T) {
	var permissions *driver_infrastructure.AllowedAndBlockedHosts
	all := []*host_info_util.HostInfo{hostWithRole(t, "writer-1", host_info_util.WRITER)}
	assert.Equal(t, []string{"writer-1"}, hostIds(permissions.FilterHosts(all)))
}

// TestAllowedAndBlockedHosts_FilterHostsDoesNotMutateInput guards against the filter aliasing or
// reordering the caller's slice: GetHosts hands it p.AllHosts, the live topology.
func TestAllowedAndBlockedHosts_FilterHostsDoesNotMutateInput(t *testing.T) {
	writer := hostWithRole(t, "writer-1", host_info_util.WRITER)
	reader := hostWithRole(t, "reader-1", host_info_util.READER)
	all := []*host_info_util.HostInfo{writer, reader}

	permissions := driver_infrastructure.NewAllowedAndBlockedHosts(nil, map[string]bool{"writer-1": true})
	filtered := permissions.FilterHosts(all)

	assert.Equal(t, []string{"reader-1"}, hostIds(filtered))
	assert.Equal(t, []string{"writer-1", "reader-1"}, hostIds(all), "the input topology was mutated")
}
