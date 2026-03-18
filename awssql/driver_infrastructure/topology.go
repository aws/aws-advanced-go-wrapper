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
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
)

// TopologyStorageType is the storage type descriptor for cluster topology data.
var TopologyStorageType = &StorageTypeDescriptor[*Topology]{
	TypeKey:       "Topology",
	TTL:           5 * time.Minute,
	RenewOnAccess: false,
}

// Topology wraps a list of hosts representing the cluster topology.
// This type is stored in the StorageService under TopologyStorageType.
type Topology struct {
	hosts []*host_info_util.HostInfo
}

// NewTopology creates a new Topology with the given hosts.
func NewTopology(hosts []*host_info_util.HostInfo) *Topology {
	return &Topology{hosts: hosts}
}

// GetHosts returns the list of hosts in this topology.
func (t *Topology) GetHosts() []*host_info_util.HostInfo {
	if t == nil {
		return nil
	}
	return t.hosts
}

// HashCode returns a hash code for this Topology based on its hosts.
// Unlike Fingerprint, this is order-sensitive — the same hosts in a different
// order will produce a different hash.
func (t *Topology) HashCode() uint64 {
	if t == nil || len(t.hosts) == 0 {
		return 0
	}

	h := fnv.New64a()
	for _, host := range t.hosts {
		if host != nil {
			h.Write([]byte(host.GetHostAndPort()))
			h.Write([]byte(string(host.Role)))
			h.Write([]byte(string(host.Availability)))
		}
	}
	return h.Sum64()
}

// Equals returns true if this Topology equals another Topology.
func (t *Topology) Equals(other *Topology) bool {
	if t == other {
		return true
	}
	if t == nil || other == nil {
		return false
	}
	if len(t.hosts) != len(other.hosts) {
		return false
	}

	for i, host := range t.hosts {
		otherHost := other.hosts[i]
		if host == nil && otherHost == nil {
			continue
		}
		if host == nil || otherHost == nil {
			return false
		}
		if !host.Equals(otherHost) {
			return false
		}
	}
	return true
}

// Key returns an order-independent string representation of this topology
// based on (Host, Port, Availability, Role). Weight is intentionally excluded
// since it fluctuates and shouldn't affect topology equality.
func (t *Topology) Key() string {
	if t == nil || len(t.hosts) == 0 {
		return ""
	}
	sorted := make([]*host_info_util.HostInfo, len(t.hosts))
	copy(sorted, t.hosts)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Host < sorted[j].Host })
	var b strings.Builder
	for _, h := range sorted {
		fmt.Fprintf(&b, "%s:%d:%s:%s|", h.Host, h.Port, h.Availability, h.Role)
	}
	return b.String()
}
