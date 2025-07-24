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

type AllowedAndBlockedHosts struct {
	allowedHostIds map[string]bool
	blockedHostIds map[string]bool
}

func NewAllowedAndBlockedHosts(
	allowedHostIds map[string]bool,
	blockedHostIds map[string]bool) *AllowedAndBlockedHosts {
	var allowedHostIdsToSet map[string]bool
	var blockedHostIdsToSet map[string]bool

	if allowedHostIds != nil && len(allowedHostIds) > 0 {
		allowedHostIdsToSet = allowedHostIds
	}
	if blockedHostIds != nil && len(blockedHostIds) > 0 {
		blockedHostIdsToSet = blockedHostIds
	}
	return &AllowedAndBlockedHosts{
		allowedHostIds: allowedHostIdsToSet,
		blockedHostIds: blockedHostIdsToSet,
	}
}

func (a *AllowedAndBlockedHosts) GetAllowedHostIds() map[string]bool {
	return a.allowedHostIds
}

func (a *AllowedAndBlockedHosts) GetBlockedHostIds() map[string]bool {
	return a.blockedHostIds
}
