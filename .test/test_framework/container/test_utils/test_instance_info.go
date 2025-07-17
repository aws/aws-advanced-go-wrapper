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

package test_utils

import (
	"fmt"
	"strconv"
)

type TestInstanceInfo struct {
	instanceId string
	host       string
	port       int
}

func (t TestInstanceInfo) InstanceId() string {
	return t.instanceId
}

func (t TestInstanceInfo) Host() string {
	return t.host
}

func (t TestInstanceInfo) Port() int {
	return t.port
}

func (t TestInstanceInfo) Url() string {
	return t.host + ":" + strconv.Itoa(t.port)
}

func NewTestInstanceInfo(instanceMap map[string]any) (info TestInstanceInfo, err error) {
	instanceId, ok := instanceMap["instanceId"].(string)
	host, ok2 := instanceMap["host"].(string)
	port, ok3 := instanceMap["port"].(float64)

	if !ok || !ok2 || !ok3 {
		err = fmt.Errorf("unable to cast a required property of new TestInstanceInfo from %v", instanceMap)
		return
	}

	return TestInstanceInfo{
		instanceId: instanceId,
		host:       host,
		port:       int(port),
	}, nil
}
