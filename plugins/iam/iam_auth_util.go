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

package iam

import (
	"awssql/host_info_util"
	"awssql/region_util"
	"fmt"
)

func GetIamHost(iamHost string, hostInfo host_info_util.HostInfo) string {
	if iamHost != "" {
		return iamHost
	}
	return hostInfo.Host
}

func GetIamPort(iamDefaultPort int, hostInfo host_info_util.HostInfo, dialectDefaultPort int) int {
	if iamDefaultPort > 0 {
		return iamDefaultPort
	} else if hostInfo.IsPortSpecified() {
		return hostInfo.Port
	} else {
		return dialectDefaultPort
	}
}

func GetCacheKey(user string, hostname string, port int, region region_util.Region) string {
	return fmt.Sprintf("%s:%s:%d:%s", region, hostname, port, user)
}
