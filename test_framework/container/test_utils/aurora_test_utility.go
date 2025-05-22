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
	awsDriver "awssql/driver"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
)

const writerChangeTimeout int = 10
const RETRY_CONNECTION_TO_WRITER_WITH_CLUSTER_ENDPOINT_TIME time.Duration = 30 * time.Second

type AuroraTestUtility struct {
	client *rds.Client
}

func NewAuroraTestUtility(region string) *AuroraTestUtility {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		slog.Error("Failed to load AWS configuration", "error", err)
	}

	return &AuroraTestUtility{
		client: rds.NewFromConfig(cfg),
	}
}

func (a AuroraTestUtility) waitUntilInstanceHasDesiredStatus(instanceId string, allowedStatuses ...string) error {
	instanceInfo, err := a.getDbInstance(instanceId)
	if err != nil {
		return fmt.Errorf("Invalid instance %s. Error: %e.", instanceId, err)
	}
	status := instanceInfo.DBInstanceStatus
	waitTillTime := time.Now().Add(15 * time.Minute)
	for status != nil && !slices.Contains(allowedStatuses, strings.ToLower(*status)) && waitTillTime.After(time.Now()) {
		time.Sleep(5 * time.Second)
		instanceInfo, err = a.getDbInstance(instanceId)
		if err == nil && instanceInfo.DBInstanceStatus != nil {
			status = instanceInfo.DBInstanceStatus
			slog.Info(fmt.Sprintf("Instance %s, status: %s.", instanceId, *status))
		}
	}

	if status == nil {
		unableToBeFoundMessage := "unable to be found"
		status = &unableToBeFoundMessage
	}
	if !slices.Contains(allowedStatuses, strings.ToLower(*status)) {
		return fmt.Errorf("Instance %s status is still %s.", instanceId, *status)
	}
	slog.Info(fmt.Sprintf("Instance %s, status: %s.", instanceId, *status))
	return nil
}

func (a AuroraTestUtility) getDbInstance(instanceId string) (instance types.DBInstance, err error) {
	command := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &instanceId,
		Filters: []types.Filter{
			{
				Name:   aws.String("db-instance-id"),
				Values: []string{instanceId},
			},
		},
	}
	resp, err := a.client.DescribeDBInstances(context.TODO(), command)
	if err != nil || resp.DBInstances == nil {
		slog.Warn(err.Error())
		return
	}
	return resp.DBInstances[0], nil
}

func (a AuroraTestUtility) rebootInstance(instanceId string) {
	attempts := 5
	for attempts > 0 {
		command := &rds.RebootDBInstanceInput{
			DBInstanceIdentifier: aws.String(instanceId),
		}
		_, err := a.client.RebootDBInstance(context.TODO(), command)
		if err != nil {
			slog.Debug(fmt.Sprintf("RebootDBInstance %s failed: %s.", instanceId, err.Error()))
		} else {
			// Successfully sent RebootDbInstance command.
			return
		}
		attempts--
	}
}

func (a AuroraTestUtility) waitUntilClusterHasDesiredStatus(clusterId string, desiredStatus string) error {
	clusterInfo, err := a.getDbCluster(clusterId)
	if err != nil {
		return fmt.Errorf("Invalid cluster %s.", clusterId)
	}
	status := clusterInfo.Status
	for !doesStatusMatch(status, desiredStatus) {
		// Keep trying until cluster has desired status.
		time.Sleep(time.Second)
		clusterInfo, err = a.getDbCluster(clusterId)
		if err == nil {
			status = clusterInfo.Status
		}
	}
	return nil
}

func doesStatusMatch(currentStatus *string, desiredStatus string) bool {
	if currentStatus != nil && *currentStatus == desiredStatus {
		return true
	}
	return false
}

func (a AuroraTestUtility) IsDbInstanceWriter(instanceId string, clusterId string) bool {
	writerId, err := a.GetClusterWriterInstanceId(clusterId)
	if err == nil && writerId == instanceId {
		return true
	}
	if err != nil {
		slog.Warn(fmt.Sprintf("Unable to gather writer instance id, returning false for match. Error: %s.", err.Error()))
	}
	return false
}

func (a AuroraTestUtility) GetClusterWriterInstanceId(clusterId string) (string, error) {
	if clusterId == "" {
		env, err := GetCurrentTestEnvironment()
		if err != nil {
			return "", errors.New("Unable to determine clusterId.")
		}
		clusterId = env.info.auroraClusterName
	}

	clusterInfo, err := a.getDbCluster(clusterId)
	if err != nil || clusterInfo.DBClusterMembers == nil {
		return "", fmt.Errorf("Invalid cluster %s.", clusterId)
	}
	members := clusterInfo.DBClusterMembers

	instance := members[slices.IndexFunc(members, func(instance types.DBClusterMember) bool { return *instance.IsClusterWriter })]
	if instance.DBInstanceIdentifier == nil || *instance.DBInstanceIdentifier == "" {
		return "", errors.New("Can not find writer")
	}
	return *instance.DBInstanceIdentifier, nil
}

func (a AuroraTestUtility) getDbCluster(clusterId string) (cluster types.DBCluster, err error) {
	command := &rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterId),
	}

	resp, err := a.client.DescribeDBClusters(context.TODO(), command)
	if err != nil {
		slog.Debug(fmt.Sprintf("Error when calling DescribeDBClusters: %s.", err.Error()))
		return
	}
	return resp.DBClusters[0], nil
}

func (a AuroraTestUtility) FailoverClusterAndWaitTillWriterChanged(initialWriter string, clusterId string, targetWriterId string) (err error) {
	env, err := GetCurrentTestEnvironment()
	if err != nil {
		return err
	}

	if clusterId == "" {
		clusterId = env.Info().AuroraClusterName()
	}

	if initialWriter == "" {
		initialWriter, err = a.GetClusterWriterInstanceId(clusterId)
		if err != nil {
			return err
		}
		if initialWriter == "" {
			return errors.New("unable to gather initial writer instance id")
		}
	}

	clusterEndpoint := env.Info().DatabaseInfo.ClusterEndpoint
	initialAddress, err := retrieveIpAddress(clusterEndpoint)
	if err != nil {
		return err
	}

	slog.Debug(fmt.Sprintf("Triggering failover of cluster %s.", clusterId))
	failoverErr := a.failoverClusterToTarget(clusterId, &targetWriterId)
	if failoverErr != nil {
		return failoverErr
	}

	clusterAddress, err := retrieveIpAddress(clusterEndpoint)
	if err != nil {
		return err
	}
	// TODO: potentially update so returns a warning but not an error if writer has changed but ip hasn't.
	stopTime := time.Now().Add(time.Duration(writerChangeTimeout) * time.Minute)
	slog.Debug(fmt.Sprintf("Waiting for ip address of %s to change after trigging failover for %d minutes.", clusterEndpoint, writerChangeTimeout))
	for clusterAddress == initialAddress && time.Now().Before(stopTime) {
		time.Sleep(time.Second)
		clusterAddress, err = retrieveIpAddress(clusterEndpoint)
		if err != nil {
			return err
		}
	}
	if !time.Now().Before(stopTime) {
		slog.Info("check for updated IP address timed out after 10 minutes")
		return fmt.Errorf("ip did not change in %d minutes following failover command", writerChangeTimeout)
	}
	slog.Debug(fmt.Sprintf("Ip address of %s has updated after failover.", clusterEndpoint))

	slog.Debug(fmt.Sprintf("Waiting for writer %s to change for %d minutes.", initialWriter, writerChangeTimeout))
	if !a.writerChanged(initialWriter, clusterId, writerChangeTimeout) {
		return fmt.Errorf("writer did not change in %d minutes following failover command", writerChangeTimeout)
	}
	slog.Debug(fmt.Sprintf("Writer of cluster %s has updated after failover.", clusterId))

	return nil
}

func (a AuroraTestUtility) failoverClusterToTarget(clusterId string, targetWriterId *string) error {
	env, err := GetCurrentTestEnvironment()
	if err != nil {
		return err
	}

	err = a.waitUntilClusterHasDesiredStatus(clusterId, "available")
	if err != nil {
		return err
	}

	command := &rds.FailoverDBClusterInput{
		DBClusterIdentifier:        &clusterId,
		TargetDBInstanceIdentifier: targetWriterId,
	}

	remainingAttempts := 10
	auroraUtility := NewAuroraTestUtility(env.info.Region)
	for remainingAttempts > 0 {
		remainingAttempts--
		resp, err := a.client.FailoverDBCluster(context.TODO(), command)
		if err == nil && resp.DBCluster != nil {
			err = auroraUtility.waitUntilClusterHasDesiredStatus(clusterId, "available")
			if err != nil {
				continue
			}
			writerId, err := auroraUtility.GetClusterWriterInstanceId(clusterId)
			if err != nil {
				continue
			}
			env.info.DatabaseInfo.moveInstanceFirst(writerId, false)
			env.info.ProxyDatabaseInfo.moveInstanceFirst(writerId, true)
			return nil
		}
		time.Sleep(time.Second)
	}
	return errors.New("unable to failover in 10 attempts")
}

func (a AuroraTestUtility) writerChanged(initialWriter string, clusterId string, timeoutMinutes int) bool {
	stopTime := time.Now().Add(time.Minute * time.Duration(timeoutMinutes))

	currentWriterId, _ := a.GetClusterWriterInstanceId(clusterId)

	for initialWriter == currentWriterId && time.Now().Before(stopTime) {
		time.Sleep(3 * time.Second)
		currentWriterId, _ = a.GetClusterWriterInstanceId(clusterId)
	}
	return initialWriter != currentWriterId
}

func retrieveIpAddress(clusterEndpoint string) (string, error) {
	clusterAddresses, err := net.LookupHost(clusterEndpoint)
	if err != nil || len(clusterAddresses) == 0 {
		return "", errors.New("unable to lookup current ip address")
	}
	return clusterAddresses[0], nil
}

func ConnectToTheWriterWithClusterEndpoint(dsn string, environment *TestEnvironment, auroraTestUtility *AuroraTestUtility,
	t *testing.T) (driver.Conn, *awsDriver.AwsWrapperDriver) {
	wrapperDriver := &awsDriver.AwsWrapperDriver{}

	endRetryTimeNano := time.Now().Add(RETRY_CONNECTION_TO_WRITER_WITH_CLUSTER_ENDPOINT_TIME)
	var conn driver.Conn
	var err error
	for time.Now().Before(endRetryTimeNano) {
		conn, err = wrapperDriver.Open(dsn)
		if err != nil {
			slog.Debug("Unable to open connection with dsn.")
			continue
		}
		instanceId, err := ExecuteInstanceQuery(environment.Info().Request.Engine, environment.Info().Request.Deployment, conn)
		assert.Nil(t, err)
		writerId, err := auroraTestUtility.GetClusterWriterInstanceId("")
		assert.Nil(t, err)
		slog.Debug(fmt.Sprintf("Writer instance id: %s. Connected to: %s.", writerId, instanceId))
		if writerId == instanceId {
			slog.Debug("Connected to the writer!")
			break
		}
		err = conn.Close()
		slog.Debug(fmt.Sprintf("Closing connection, err: %e.", err))
	}
	return conn, wrapperDriver
}

func ConnectToTheWriterWithClusterEndpointDB(dsn string, environment *TestEnvironment, auroraTestUtility *AuroraTestUtility, t *testing.T) *sql.DB {
	endRetryTimeNano := time.Now().Add(RETRY_CONNECTION_TO_WRITER_WITH_CLUSTER_ENDPOINT_TIME)
	var db *sql.DB
	var err error
	for time.Now().Before(endRetryTimeNano) {
		db, err = sql.Open("awssql", dsn)
		if err != nil {
			slog.Debug("Unable to open connection with dsn.")
			continue
		}
		instanceId, err := ExecuteInstanceQueryDB(environment.Info().Request.Engine, environment.Info().Request.Deployment, db)
		assert.Nil(t, err)
		writerId, err := auroraTestUtility.GetClusterWriterInstanceId("")
		assert.Nil(t, err)
		slog.Debug(fmt.Sprintf("Writer instance id: %s. Connected to: %s.", writerId, instanceId))
		if writerId == instanceId {
			slog.Debug("Connected to the writer!")
			break
		}
		err = db.Close()
		slog.Debug(fmt.Sprintf("Closing connection, err: %e.", err))
	}
	return db
}

func BasicCleanupAfterBasicSetup(t *testing.T) func() {
	err := BasicSetup(t.Name())
	assert.Nil(t, err)

	return func() {
		BasicCleanup(t.Name())
	}
}

func BasicSetup(name string) error {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Info(fmt.Sprintf("Test started: %s.", name))

	EnableAllConnectivity()
	return VerifyClusterStatus()
}

func BasicCleanup(name string) {
	awsDriver.ClearCaches()
	slog.Info(fmt.Sprintf("Test finished: %s.", name))
}

func FailoverSetup(t *testing.T) (*AuroraTestUtility, error) {
	environment, err := GetCurrentTestEnvironment()
	assert.Nil(t, err)
	if environment.Info().Request.InstanceCount < 2 {
		t.Skipf("Skipping integration test %s, instanceCount = %v.", t.Name(), environment.Info().Request.InstanceCount)
	}
	auroraTestUtility := NewAuroraTestUtility(environment.Info().Region)
	return auroraTestUtility, BasicSetup(t.Name())
}
