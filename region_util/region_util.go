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

package region_util

import (
	"awssql/property_util"
	"awssql/utils"
	"strings"
)

func GetRegion(host string, props map[string]string, prop property_util.AwsWrapperProperty) Region {
	region := GetRegionFromProps(props, prop)
	if region != "" {
		return region
	} else {
		return GetRegionFromHost(host)
	}
}

func GetRegionFromProps(props map[string]string, prop property_util.AwsWrapperProperty) Region {
	regionString := property_util.GetVerifiedWrapperPropertyValue[string](props, prop)
	if regionString == "" {
		return ""
	}
	return GetRegionFromRegionString(regionString)
}

func GetRegionFromHost(host string) Region {
	regionString := utils.GetRdsRegion(host)
	if regionString == "" {
		return ""
	}

	return GetRegionFromRegionString(regionString)
}

func GetRegionFromRegionString(regionString string) Region {
	if regionString == "" {
		return ""
	}

	regionFound, ok := ALL_REGIONS[Region(strings.ToLower(regionString))]
	if regionFound && ok {
		return Region(strings.ToLower(regionString))
	}
	return ""
}
