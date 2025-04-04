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

type Region string

const (
	AP_SOUTH_2      Region = "ap-south-2"
	AP_SOUTH_1      Region = "ap-south-1"
	EU_SOUTH_1      Region = "eu-south-1"
	EU_SOUTH_2      Region = "eu-south-2"
	US_GOV_EAST_1   Region = "us-gov-east-1"
	ME_CENTRAL_1    Region = "me-central-1"
	IL_CENTRAL_1    Region = "il-central-1"
	US_ISOF_SOUTH_1 Region = "us-isof-south-1"
	CA_CENTRAL_1    Region = "ca-central-1"
	MX_CENTRAL_1    Region = "mx-central-1"
	EU_CENTRAL_1    Region = "eu-central-1"
	US_ISO_WEST_1   Region = "us-iso-west-1"
	EU_CENTRAL_2    Region = "eu-central-2"
	EU_ISOE_WEST_1  Region = "eu-isoe-west-1"
	US_WEST_1       Region = "us-west-1"
	US_WEST_2       Region = "us-west-2"
	AF_SOUTH_1      Region = "af-south-1"
	EU_NORTH_1      Region = "eu-north-1"
	EU_WEST_3       Region = "eu-west-3"
	EU_WEST_2       Region = "eu-west-2"
	EU_WEST_1       Region = "eu-west-1"
	AP_NORTHEAST_3  Region = "ap-northeast-3"
	AP_NORTHEAST_2  Region = "ap-northeast-2"
	AP_NORTHEAST_1  Region = "ap-northeast-1"
	ME_SOUTH_1      Region = "me-south-1"
	SA_EAST_1       Region = "sa-east-1"
	AP_EAST_1       Region = "ap-east-1"
	CN_NORTH_1      Region = "cn-north-1"
	CA_WEST_1       Region = "ca-west-1"
	US_GOV_WEST_1   Region = "us-gov-west-1"
	AP_SOUTHEAST_1  Region = "ap-southeast-1"
	AP_SOUTHEAST_2  Region = "ap-southeast-2"
	US_ISO_EAST_1   Region = "us-iso-east-1"
	AP_SOUTHEAST_3  Region = "ap-southeast-3"
	AP_SOUTHEAST_4  Region = "ap-southeast-4"
	AP_SOUTHEAST_5  Region = "ap-southeast-5"
	US_EAST_1       Region = "us-east-1"
	US_EAST_2       Region = "us-east-2"
	AP_SOUTHEAST_7  Region = "ap-southeast-7"
	CN_NORTHWEST_1  Region = "cn-northwest-1"
	US_ISOB_EAST_1  Region = "us-isob-east-1"
	US_ISOF_EAST_1  Region = "us-isof-east-1"
)

var ALL_REGIONS = map[Region]bool{
	AP_SOUTH_2:      true,
	AP_SOUTH_1:      true,
	EU_SOUTH_1:      true,
	EU_SOUTH_2:      true,
	US_GOV_EAST_1:   true,
	ME_CENTRAL_1:    true,
	IL_CENTRAL_1:    true,
	US_ISOF_SOUTH_1: true,
	CA_CENTRAL_1:    true,
	MX_CENTRAL_1:    true,
	EU_CENTRAL_1:    true,
	US_ISO_WEST_1:   true,
	EU_CENTRAL_2:    true,
	EU_ISOE_WEST_1:  true,
	US_WEST_1:       true,
	US_WEST_2:       true,
	AF_SOUTH_1:      true,
	EU_NORTH_1:      true,
	EU_WEST_3:       true,
	EU_WEST_2:       true,
	EU_WEST_1:       true,
	AP_NORTHEAST_3:  true,
	AP_NORTHEAST_2:  true,
	AP_NORTHEAST_1:  true,
	ME_SOUTH_1:      true,
	SA_EAST_1:       true,
	AP_EAST_1:       true,
	CN_NORTH_1:      true,
	CA_WEST_1:       true,
	US_GOV_WEST_1:   true,
	AP_SOUTHEAST_1:  true,
	AP_SOUTHEAST_2:  true,
	US_ISO_EAST_1:   true,
	AP_SOUTHEAST_3:  true,
	AP_SOUTHEAST_4:  true,
	AP_SOUTHEAST_5:  true,
	US_EAST_1:       true,
	US_EAST_2:       true,
	AP_SOUTHEAST_7:  true,
	CN_NORTHWEST_1:  true,
	US_ISOB_EAST_1:  true,
	US_ISOF_EAST_1:  true,
}
