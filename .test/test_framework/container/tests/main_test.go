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

package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
)

// TestMain prints which target driver, engine and deployment the run resolved.
func TestMain(m *testing.M) {
	environment, err := test_utils.GetCurrentTestEnvironment()
	if err != nil {
		fmt.Printf("Unable to read the test environment: %s.\n", err.Error())
		os.Exit(1)
	}

	request := environment.Info().Request
	// Resolved once here so an unusable TARGET_DRIVER fails the run immediately,
	// with one clear message, rather than panicking inside whichever test happens
	// to open a connection first.
	targetDriver, err := test_utils.TargetDriverForEngine(request.Engine)
	if err != nil {
		fmt.Printf("Unable to determine the target driver: %s.\n", err.Error())
		os.Exit(1)
	}

	fmt.Printf("target driver=%s engine=%s deployment=%s\n", targetDriver, request.Engine, request.Deployment)

	os.Exit(m.Run())
}
