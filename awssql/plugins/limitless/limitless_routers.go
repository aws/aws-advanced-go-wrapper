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

package limitless

import (
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

// LimitlessRouters wraps a list of limitless router hosts for storage.
type LimitlessRouters struct {
	Hosts []*host_info_util.HostInfo
}

// NewLimitlessRouters creates a new LimitlessRouters instance.
func NewLimitlessRouters(hosts []*host_info_util.HostInfo) *LimitlessRouters {
	return &LimitlessRouters{Hosts: hosts}
}

// GetHosts returns the list of router hosts.
func (r *LimitlessRouters) GetHosts() []*host_info_util.HostInfo {
	return r.Hosts
}

// LimitlessRoutersStorageType is the type-safe descriptor for storing LimitlessRouters.
// Used with StorageService to manage limitless router cache.
var LimitlessRoutersStorageType = &driver_infrastructure.StorageTypeDescriptor[*LimitlessRouters]{
	TypeKey:       "LimitlessRouters",
	TTL:           10 * time.Minute, // Default, will be overridden by registration
	RenewOnAccess: true,
	ShouldDispose: nil,
	OnDispose:     nil,
}
