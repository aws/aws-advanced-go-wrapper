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

package examples

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
)

func globalDatabasesPostgreSQLExample() {
	// Writer connection using global cluster endpoint
	writerHost := "my-global-db.global-xyz.global.rds.amazonaws.com"
	writerDsn := fmt.Sprintf(
		"host=%s port=5432 user=username dbname=mydb password=password clusterId=1 databaseDialect=global-aurora-pg plugins=failover,efm globalClusterInstanceHostPatterns=?.abc123.us-east-1.rds.amazonaws.com,?.def456.us-west-2.rds.amazonaws.com",
		writerHost,
	)

	writerDb, err := sql.Open("awssql-pgx", writerDsn)
	if err != nil {
		log.Fatal("Failed to open writer connection:", err)
	}
	defer writerDb.Close()

	// Reader connection using cluster reader endpoint
	readerHost := "my-cluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com"
	readerDsn := fmt.Sprintf(
		"host=%s port=5432 user=username dbname=mydb password=password clusterId=1 databaseDialect=global-aurora-pg plugins=failover,efm globalClusterInstanceHostPatterns=?.abc123.us-east-1.rds.amazonaws.com,?.def456.us-west-2.rds.amazonaws.com failoverMode=strict-reader",
		readerHost,
	)

	readerDb, err := sql.Open("awssql-pgx", readerDsn)
	if err != nil {
		log.Fatal("Failed to open reader connection:", err)
	}
	defer readerDb.Close()
}
