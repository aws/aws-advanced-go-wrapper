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
	"testing"

	auth_helpers "github.com/aws/aws-advanced-go-wrapper/auth-helpers"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/stretchr/testify/assert"
)

func TestValidateAuthParams(t *testing.T) {
	errMsgCode := "AuthHelpers.missingRequiredParameters"

	// Test for valid Auth parameters
	err := auth_helpers.ValidateAuthParams("pluginCode",
		"dbUser",
		"idpUsername",
		"idpPassword",
		"idpEndpoint",
		"iamRoleArn",
		"iamIdpArn",
		"appId")

	assert.NoError(t, err)

	// Test for one missing param
	missingParam := []string{property_util.DB_USER.Name}
	err = auth_helpers.ValidateAuthParams("pluginCode",
		"",
		"idpUsername",
		"idpPassword",
		"idpEndpoint",
		"iamRoleArn",
		"iamIdpArn",
		"appId")

	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage(errMsgCode, "pluginCode", missingParam), err.Error())

	// Test for missing multiple params
	missingParam = []string{property_util.DB_USER.Name,
		property_util.IDP_USERNAME.Name,
		property_util.IDP_PASSWORD.Name,
		property_util.IDP_ENDPOINT.Name,
		property_util.IAM_ROLE_ARN.Name,
		property_util.IAM_IDP_ARN.Name,
	}
	err = auth_helpers.ValidateAuthParams("pluginCode",
		"",
		"",
		"",
		"",
		"",
		"",
		"")

	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage(errMsgCode, "pluginCode", missingParam), err.Error())

	// Test for all missing params
	missingParam = []string{property_util.DB_USER.Name, property_util.IDP_USERNAME.Name, property_util.IDP_ENDPOINT.Name}
	err = auth_helpers.ValidateAuthParams("pluginCode",
		"",
		"",
		"idpPassword",
		"",
		"iamRoleArn",
		"iamIdpArn",
		"appId")

	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage(errMsgCode, "pluginCode", missingParam), err.Error())

	// Test for missing appid for plugin that does not require app id
	err = auth_helpers.ValidateAuthParams("pluginCode",
		"dbUser",
		"idpUsername",
		"idpPassword",
		"idpEndpoint",
		"iamRoleArn",
		"iamIdpArn",
		"")

	assert.NoError(t, err)

	// Test for missing appid for plugin that requires appid
	missingParam = []string{property_util.APP_ID.Name}
	err = auth_helpers.ValidateAuthParams("okta",
		"dbUser",
		"idpUsername",
		"idpPassword",
		"idpEndpoint",
		"iamRoleArn",
		"iamIdpArn",
		"")

	assert.Error(t, err)
	assert.Equal(t, error_util.GetMessage(errMsgCode, "okta", missingParam), err.Error())
}
