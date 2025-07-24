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

package custom_endpoint

import (
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

type CustomEndpointInfo struct {
	endpointIdentifier string
	clusterIdentifier  string
	url                string
	roleType           RoleType
	memberListType     MemberListType
	members            map[string]bool
}

type RoleType string

const (
	ANY    RoleType = "ANY"
	WRITER RoleType = "WRITER"
	READER RoleType = "READER"
)

type MemberListType string

const (
	STATIC_LIST    MemberListType = "STATIC_LIST"
	EXCLUSION_LIST MemberListType = "EXCLUSION_LIST"
)

func NewCustomEndpointInfo(endpoint types.DBClusterEndpoint) *CustomEndpointInfo {
	var members []string
	var memberListType MemberListType

	if len(endpoint.StaticMembers) > 1 {
		members = endpoint.StaticMembers
		memberListType = STATIC_LIST
	} else {
		members = endpoint.ExcludedMembers
		memberListType = EXCLUSION_LIST
	}

	return &CustomEndpointInfo{
		endpointIdentifier: *endpoint.DBClusterEndpointIdentifier,
		clusterIdentifier:  *endpoint.DBClusterIdentifier,
		url:                *endpoint.Endpoint,
		roleType:           RoleType(strings.ToUpper(*endpoint.CustomEndpointType)),
		memberListType:     memberListType,
		members:            stringSliceToSetMap(members),
	}
}

func (a *CustomEndpointInfo) Equals(b *CustomEndpointInfo) bool {
	return a.endpointIdentifier == b.endpointIdentifier &&
		a.clusterIdentifier == b.clusterIdentifier &&
		a.url == b.url &&
		a.roleType == b.roleType &&
		a.memberListType == b.memberListType &&
		reflect.DeepEqual(a.members, b.members)
}

func (a *CustomEndpointInfo) GetStaticMembers() map[string]bool {
	if STATIC_LIST == a.memberListType {
		return a.members
	} else {
		return nil
	}
}

func (a *CustomEndpointInfo) GetExcludedMembers() map[string]bool {
	if EXCLUSION_LIST == a.memberListType {
		return a.members
	} else {
		return nil
	}
}

func stringSliceToSetMap(stringSlice []string) map[string]bool {
	setMapToReturn := make(map[string]bool)
	for _, str := range stringSlice {
		setMapToReturn[str] = true
	}
	return setMapToReturn
}
