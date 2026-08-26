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
	"log/slog"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
)

// AllowedAndBlockedHostsStorageType is the storage type descriptor for allowed/blocked host data.
var AllowedAndBlockedHostsStorageType = &StorageTypeDescriptor[*AllowedAndBlockedHosts]{
	TypeKey:       "AllowedAndBlockedHosts",
	TTL:           5 * time.Minute,
	RenewOnAccess: false,
}

type AllowedAndBlockedHosts struct {
	allowedHostIds map[string]bool
	blockedHostIds map[string]bool
	requiredRole   host_info_util.HostRole
}

// NewAllowedAndBlockedHosts builds a host permission set with no role requirement.
//
// allowedHostIds: if empty, every host not in blockedHostIds is allowed.
// blockedHostIds: if empty, every host in allowedHostIds is allowed; if both are empty there is no
// host-id restriction.
//
// Deprecated: use NewAllowedAndBlockedHostsWithRole, passing host_info_util.UNKNOWN when the hosts need
// no particular role. This form is kept so existing callers continue to compile.
func NewAllowedAndBlockedHosts(
	allowedHostIds map[string]bool,
	blockedHostIds map[string]bool) *AllowedAndBlockedHosts {
	return NewAllowedAndBlockedHostsWithRole(allowedHostIds, blockedHostIds, host_info_util.UNKNOWN)
}

// NewAllowedAndBlockedHostsWithRole builds a host permission set that also requires a host role.
//
// requiredRole: the role a host must have to be used, or host_info_util.UNKNOWN for no requirement.
func NewAllowedAndBlockedHostsWithRole(
	allowedHostIds map[string]bool,
	blockedHostIds map[string]bool,
	requiredRole host_info_util.HostRole) *AllowedAndBlockedHosts {
	var allowedHostIdsToSet map[string]bool
	var blockedHostIdsToSet map[string]bool

	if len(allowedHostIds) > 0 {
		allowedHostIdsToSet = allowedHostIds
	}
	if len(blockedHostIds) > 0 {
		blockedHostIdsToSet = blockedHostIds
	}
	return &AllowedAndBlockedHosts{
		allowedHostIds: allowedHostIdsToSet,
		blockedHostIds: blockedHostIdsToSet,
		requiredRole:   requiredRole,
	}
}

func (a *AllowedAndBlockedHosts) GetAllowedHostIds() map[string]bool {
	return a.allowedHostIds
}

func (a *AllowedAndBlockedHosts) GetBlockedHostIds() map[string]bool {
	return a.blockedHostIds
}

// GetRequiredRole returns the role a host must have to be used, or host_info_util.UNKNOWN when there is
// no requirement.
func (a *AllowedAndBlockedHosts) GetRequiredRole() host_info_util.HostRole {
	return a.requiredRole
}

// FilterHosts applies these permissions to a host list, so every path handing hosts to selection filters
// identically.
//
// The result may be the input slice itself, so callers must not mutate it, and may be empty when none of
// the endpoint's members are in the topology.
func (a *AllowedAndBlockedHosts) FilterHosts(hosts []*host_info_util.HostInfo) []*host_info_util.HostInfo {
	if a == nil {
		return hosts
	}

	if len(a.allowedHostIds) > 0 {
		hosts = utils.FilterSlice(hosts, func(item *host_info_util.HostInfo) bool {
			value, ok := a.allowedHostIds[item.HostId]
			return ok && value
		})
	}

	if len(a.blockedHostIds) > 0 {
		hosts = utils.FilterSlice(hosts, func(item *host_info_util.HostInfo) bool {
			value, ok := a.blockedHostIds[item.HostId]
			return !ok || !value
		})
	}

	// Applied independently of the two id lists. Nesting it inside them would leave the requirement
	// unenforced when both are empty, which is what a READER endpoint excluding nothing produces.
	//
	// Gated on the two roles that can actually be required rather than on `!= UNKNOWN`, because
	// HostRole's zero value is "" and a struct literal would otherwise filter every host away.
	if a.requiredRole == host_info_util.READER || a.requiredRole == host_info_util.WRITER {
		before := len(hosts)
		hosts = utils.FilterSlice(hosts, func(item *host_info_util.HostInfo) bool {
			return item.Role == a.requiredRole
		})
		if removed := before - len(hosts); removed > 0 && slog.Default().Enabled(context.TODO(), slog.LevelDebug) {
			slog.Debug(error_util.GetMessage("AllowedAndBlockedHosts.roleFilterRemovedHosts",
				removed, a.requiredRole, len(hosts)))
		}
	}

	return hosts
}
