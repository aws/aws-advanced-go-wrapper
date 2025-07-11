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
	"context"
	"fmt"
	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLimitlessValidConnectionProperties(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()

	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.Nil(t, err)

	test_utils.RequireTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)

	dsn := test_utils.GetDsn(environment, map[string]string{"plugins": "limitless"})
	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}

func TestLimitlessAndIamPlugin(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()
	awsDriver.ClearCaches()
	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)

	test_utils.RequireTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)
	test_utils.RequireTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.IAM)

	props := map[string]string{
		property_util.PLUGINS.Name:    "iam,limitless",
		property_util.USER.Name:       environment.Info().IamUsername,
		property_util.PASSWORD.Name:   "anypassword",
		property_util.IAM_REGION.Name: environment.Info().Region,
	}

	// Needed for MYSQL
	if environment.Info().Request.Engine == test_utils.MYSQL {
		props["tls"] = "skip-verify"
		props["allowCleartextPasswords"] = "true"
	}

	dsn := test_utils.GetDsn(environment, props)

	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}

func TestLimitlessAndAwsSecretsManagerPlugin(t *testing.T) {
	defer test_utils.BasicCleanupAfterBasicSetup(t)()
	awsDriver.ClearCaches()
	environment, err := test_utils.GetCurrentTestEnvironment()
	assert.NoError(t, err)

	test_utils.RequireTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.LIMITLESS_DEPLOYMENT)
	test_utils.RequireTestEnvironmentFeatures(t, environment.Info().Request.Features, test_utils.SECRETS_MANAGER)

	cfg, _ := config.LoadDefaultConfig(context.TODO())
	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.Region = environment.Info().Region
	})
	secretName := fmt.Sprintf("TestSecret-%s", uuid.New().String())
	CreateSecret(t, client, environment, secretName)
	defer DeleteSecret(t, client, environment, secretName)

	props := map[string]string{
		property_util.PLUGINS.Name:                   "awsSecretsManager,limitless",
		property_util.USER.Name:                      "incorrectUser",
		property_util.PASSWORD.Name:                  "incorrectPassword",
		property_util.SECRETS_MANAGER_SECRET_ID.Name: secretName,
		property_util.SECRETS_MANAGER_REGION.Name:    environment.Info().Region,
	}

	dsn := test_utils.GetDsn(environment, props)

	db, err := test_utils.OpenDb(environment.Info().Request.Engine, dsn)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	instanceId, err := test_utils.ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
	assert.Nil(t, err)
	assert.NotZero(t, instanceId)
}
