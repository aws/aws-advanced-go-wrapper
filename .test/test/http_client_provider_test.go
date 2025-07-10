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
	"net/http"
	"testing"
	"time"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/stretchr/testify/assert"
)

func TestGetBasicHttpClient_Config(t *testing.T) {
	client := auth_helpers.GetBasicHttpClient(5000, true, nil)

	httpClient, ok := client.(*http.Client)
	assert.True(t, ok)
	assert.Equal(t, 5*time.Second, httpClient.Timeout)

	transport, ok := httpClient.Transport.(*http.Transport)
	assert.True(t, ok)
	assert.True(t, transport.TLSClientConfig.InsecureSkipVerify)
}
