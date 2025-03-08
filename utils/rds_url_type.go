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

package utils

type RdsUrlType struct {
	IsRds        bool
	IsRdsCluster bool
}

var (
	IP_ADDRESS                          RdsUrlType = RdsUrlType{false, false}
	RDS_WRITER_CLUSTER                  RdsUrlType = RdsUrlType{true, true}
	RDS_READER_CLUSTER                  RdsUrlType = RdsUrlType{true, true}
	RDS_CUSTOM_CLUSTER                  RdsUrlType = RdsUrlType{true, true}
	RDS_PROXY                           RdsUrlType = RdsUrlType{true, false}
	RDS_INSTANCE                        RdsUrlType = RdsUrlType{true, false}
	RDS_AURORA_LIMITLESS_DB_SHARD_GROUP RdsUrlType = RdsUrlType{true, false}
	OTHER                               RdsUrlType = RdsUrlType{false, false}
)
