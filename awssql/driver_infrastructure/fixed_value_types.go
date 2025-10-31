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

package driver_infrastructure

const (
	BLUE_GREEN_PLUGIN_CODE                string = "bg"
	READ_WRITE_SPLITTING_PLUGIN_CODE      string = "readWriteSplitting"
	FAILOVER_PLUGIN_CODE                  string = "failover"
	EFM_PLUGIN_CODE                       string = "efm"
	LIMITLESS_PLUGIN_CODE                 string = "limitless"
	IAM_PLUGIN_CODE                       string = "iam"
	SECRETS_MANAGER_PLUGIN_CODE           string = "awsSecretsManager"
	ADFS_PLUGIN_CODE                      string = "federatedAuth"
	OKTA_PLUGIN_CODE                      string = "okta"
	EXECUTION_TIME_PLUGIN_CODE            string = "executionTime"
	CONNECT_TIME_PLUGIN_CODE              string = "connectTime"
	AURORA_CONNECTION_TRACKER_PLUGIN_CODE string = "auroraConnectionTracker"
	DEVELOPER_PLUGIN_CODE                 string = "dev"
)

type HostChangeOptions int

const (
	HOSTNAME                  HostChangeOptions = 0
	PROMOTED_TO_WRITER        HostChangeOptions = 1
	PROMOTED_TO_READER        HostChangeOptions = 2
	WENT_UP                   HostChangeOptions = 3
	WENT_DOWN                 HostChangeOptions = 4
	CONNECTION_OBJECT_CHANGED HostChangeOptions = 5
	INITIAL_CONNECTION        HostChangeOptions = 6
	HOST_ADDED                HostChangeOptions = 7
	HOST_CHANGED              HostChangeOptions = 8
	HOST_DELETED              HostChangeOptions = 9
)

type OldConnectionSuggestedAction string

const (
	NO_OPINION OldConnectionSuggestedAction = "no_opinion"
	DISPOSE    OldConnectionSuggestedAction = "dispose"
	PRESERVE   OldConnectionSuggestedAction = "preserve"
)

type TransactionIsolationLevel int

const (
	TRANSACTION_READ_UNCOMMITTED TransactionIsolationLevel = 0
	TRANSACTION_READ_COMMITTED   TransactionIsolationLevel = 1
	TRANSACTION_REPEATABLE_READ  TransactionIsolationLevel = 2
	TRANSACTION_SERIALIZABLE     TransactionIsolationLevel = 3
)

type DialectCode string

const (
	AURORA_MYSQL_DIALECT               string = "aurora-mysql"
	RDS_MYSQL_DIALECT                  string = "rds-mysql"
	MYSQL_DIALECT                      string = "mysql"
	RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT string = "rds-multi-az-mysql-cluster"
	AURORA_PG_DIALECT                  string = "aurora-pg"
	RDS_PG_DIALECT                     string = "rds-pg"
	PG_DIALECT                         string = "pg"
	RDS_PG_MULTI_AZ_CLUSTER_DIALECT    string = "rds-multi-az-pg-cluster"
)

type DatabaseEngine string

const (
	MYSQL DatabaseEngine = "mysql"
	PG    DatabaseEngine = "pg"
)

const (
	AWS_PGX_DRIVER_CODE   string = "awssql-pgx"
	AWS_MYSQL_DRIVER_CODE string = "awssql-mysql"
)
