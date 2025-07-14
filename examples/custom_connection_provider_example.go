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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"

	"github.com/aws/aws-advanced-go-wrapper/awssql/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	_ "github.com/aws/aws-advanced-go-wrapper/pgx-driver" // awssql pgx driver
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {
	// Database connection string
	host := "endpoint"
	port := "5432"
	user := "user"
	password := "password"
	dbName := "db"

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		host, port, user, password, dbName,
	)

	// Set a custom ConnectionProvider to be used when creating connections.
	driver_infrastructure.SetCustomConnectionProvider(&CustomConnectionProvider{
		targetDriver: &stdlib.Driver{}, // Will make connections through pgx stdlib.
		acceptedStrategies: map[string]driver_infrastructure.HostSelector{
			"first": &FirstHostSelector{}, // Only supports the "first" host selection strategy.
		},
	})

	// Open the database connection
	db, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer db.Close()

	// Execute the query
	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		log.Fatal("Error executing query:", err)
	}

	// Print the result
	fmt.Println("Query result:", result)

	// Call reset to make connections without the custom ConnectionProvider.
	driver_infrastructure.ResetCustomConnectionProvider()

	// Will open the database connection using the default ConnectionProvider.
	dbAfterReset, err := sql.Open("awssql-pgx", connStr)
	if err != nil {
		log.Fatal("There was an issue opening the database handler:", err)
	}
	defer dbAfterReset.Close()
}

// This is an example implementation of ConnectionProvider.
type CustomConnectionProvider struct {
	targetDriver       driver.Driver
	acceptedStrategies map[string]driver_infrastructure.HostSelector
}

// Adds criteria for which hosts and properties are supported by this connection provider.
// This implementation accepts any values, meaning all connections will connect through this connection provider.
func (c *CustomConnectionProvider) AcceptsUrl(hostInfo host_info_util.HostInfo, props map[string]string) bool {
	return true
}

// Creates a connection with the database.
// This implementation passes a dsn to its targetDriver to connect.
func (c *CustomConnectionProvider) Connect(hostInfo *host_info_util.HostInfo, props map[string]string, pluginService driver_infrastructure.PluginService) (driver.Conn, error) {
	dsn := "exampleDsn" // Should be constructed from the parameters.
	return c.targetDriver.Open(dsn)
}

// Adds criteria for which host selection strategies are supported.
// This implementation returns true if strategy is part of acceptedStrategies.
func (c *CustomConnectionProvider) AcceptsStrategy(strategy string) bool {
	_, ok := c.acceptedStrategies[strategy]
	return ok
}

// Selects a host from hosts using the provided strategy.
// This implementation passes the task to a HostSelector.
func (c *CustomConnectionProvider) GetHostInfoByStrategy(hosts []*host_info_util.HostInfo, role host_info_util.HostRole, strategy string, props map[string]string) (*host_info_util.HostInfo, error) {
	selector, err := c.GetHostSelectorStrategy(strategy)
	if err != nil {
		return selector.GetHost(hosts, role, props)
	}
	return nil, fmt.Errorf("Unsupported strategy.")
}

// Returns a HostSelector that will select a preferred host from a list of hosts.
// This implementation will always return the corresponding HostSelector to strategy from acceptedStrategies.
func (c *CustomConnectionProvider) GetHostSelectorStrategy(strategy string) (driver_infrastructure.HostSelector, error) {
	acceptedStrategy, ok := c.acceptedStrategies[strategy]
	if ok {
		return acceptedStrategy, nil
	}
	return nil, fmt.Errorf("No selector.")
}

// Additonal customization is possible through a custom HostSelector.
type FirstHostSelector struct {
}

// Implements a strategy to select a host from a list of hosts.
// This implementation returns the first host in the list regardless of role or props.
func (h *FirstHostSelector) GetHost(hosts []*host_info_util.HostInfo, role host_info_util.HostRole, props map[string]string) (*host_info_util.HostInfo, error) {
	if len(hosts) > 0 {
		return hosts[0], nil
	}
	return nil, fmt.Errorf("Empty host list.")
}
