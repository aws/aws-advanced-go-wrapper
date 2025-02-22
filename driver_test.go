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

package main

import (
	awsDriver "awssql/driver"
	"database/sql/driver"
	"testing"
)

func TestDummy(t *testing.T) {
	got := 1
	want := 1

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}

func TestImplementations(t *testing.T) {
	// Check for correct implementations of interfaces on left.
	var _ awsDriver.PluginManager = (*awsDriver.ConnectionPluginManager)(nil)
	var _ driver.Driver = (*awsDriver.AwsWrapperDriver)(nil)
	var _ driver.Conn = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Pinger = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ExecerContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.QueryerContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ConnPrepareContext = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.ConnBeginTx = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.SessionResetter = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Validator = (*awsDriver.AwsWrapperConn)(nil)
	var _ driver.Stmt = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.StmtExecContext = (*awsDriver.AwsWrapperStmt)(nil)
	var _ driver.StmtQueryContext = (*awsDriver.AwsWrapperStmt)(nil)
}
