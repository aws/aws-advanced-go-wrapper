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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	awsDriver "github.com/aws/aws-advanced-go-wrapper/awssql/driver"
	"github.com/aws/aws-xray-sdk-go/strategy/sampling"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/aws/aws-xray-sdk-go/xraylog"
	xray2 "go.opentelemetry.io/contrib/propagators/aws/xray"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
)

const writerChangeTimeout int = 10

type AuroraTestUtility struct {
	client *rds.Client
}

func NewAuroraTestUtility(info TestEnvironmentInfo) *AuroraTestUtility {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(info.Region),
	)

	if err != nil {
		slog.Error("Failed to load AWS configuration", "error", err)
	}

	var rdsClient *rds.Client
	if info.Endpoint() == "" {
		rdsClient = rds.NewFromConfig(cfg)
	} else {
		// RDS-specific endpoint override
		rdsClient = rds.NewFromConfig(cfg, func(o *rds.Options) {
			o.BaseEndpoint = aws.String(info.Endpoint())
		})
	}

	return &AuroraTestUtility{
		client: rdsClient,
	}
}

func (a AuroraTestUtility) waitUntilInstanceHasDesiredStatus(instanceId string, allowedStatuses ...string) error {
	instanceInfo, err := a.getDbInstance(instanceId)
	if err != nil {
		return fmt.Errorf("invalid instance %s, error: %e", instanceId, err)
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
		return fmt.Errorf("instance %s status is still %s", instanceId, *status)
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
	if err != nil || len(resp.DBInstances) == 0 {
		if err == nil {
			err = fmt.Errorf("DescribeDBInstances returned no instances")
		}
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
		return fmt.Errorf("invalid cluster %s", clusterId)
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
	slog.Debug("IsDbInstanceWriter() - instanceId:" + instanceId)
	writerId, err := a.GetClusterWriterInstanceId(clusterId)
	if err == nil && writerId == instanceId {
		return true
	}
	if err != nil {
		slog.Warn(fmt.Sprintf("unable to gather writer instance id, returning false for match, error: %s", err.Error()))
	}
	return false
}

func (a AuroraTestUtility) GetClusterWriterInstanceId(clusterId string) (string, error) {
	if clusterId == "" {
		env, err := GetCurrentTestEnvironment()
		if err != nil {
			return "", errors.New("unable to determine clusterId")
		}
		clusterId = env.info.RdsDbName()
	}
	clusterInfo, err := a.getDbCluster(clusterId)
	if err != nil || clusterInfo.DBClusterMembers == nil {
		return "", fmt.Errorf("invalid cluster %s", clusterId)
	}
	members := clusterInfo.DBClusterMembers

	instance := members[slices.IndexFunc(members, func(instance types.DBClusterMember) bool { return *instance.IsClusterWriter })]
	if instance.DBInstanceIdentifier == nil || *instance.DBInstanceIdentifier == "" {
		return "", errors.New("can not find writer")
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

func (a AuroraTestUtility) CrashInstance(initialWriter string, clusterId string, targetWriterId string) (err error) {
	env, err := GetCurrentTestEnvironment()
	deployment := env.Info().Request.Deployment

	if RDS_MULTI_AZ_CLUSTER == deployment {
		slog.Debug("CrashInstance() - RDS_MULTI_AZ_CLUSTER deployment detected. Simulating temporary failure")
		a.SimulateTemporaryFailure()
		return nil
	} else {
		slog.Debug("CrashInstance() - dbengine deployment %v detected. FailoverClusterAndWaitTillWriterChanged", deployment)
		return a.FailoverClusterAndWaitTillWriterChanged(initialWriter, clusterId, targetWriterId)
	}
}

func (a AuroraTestUtility) SimulateTemporaryFailure() {
	DisableAllProxies()
	time.Sleep(5 * time.Second)
	EnableAllProxies()
}

func (a AuroraTestUtility) FailoverClusterAndWaitTillWriterChanged(initialWriter string, clusterId string, targetWriterId string) (err error) {
	env, err := GetCurrentTestEnvironment()
	if err != nil {
		return err
	}

	if clusterId == "" {
		clusterId = env.Info().RdsDbName()
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

	slog.Debug(fmt.Sprintf("Triggering failover of cluster %s.", clusterId))
	failoverErr := a.failoverClusterToTarget(clusterId, &targetWriterId)
	if failoverErr != nil {
		return failoverErr
	}

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
	auroraUtility := NewAuroraTestUtility(env.info)
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
		} else {
			slog.Debug(fmt.Sprintf("Request to failover cluster %s with writer %s failed. Response was %v.", clusterId, *targetWriterId, resp))
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

func SetupTelemetry(env *TestEnvironment) (trace.SpanProcessor, error) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	ctx := context.Background()

	endpoint := env.Info().OtelTracesTelemetryInfo.Endpoint + ":" + env.Info().OtelTracesTelemetryInfo.EndpointPort

	traceExporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, errors.New("unable to create otlp trace exporter")
	}

	bsp := trace.NewBatchSpanProcessor(traceExporter,
		trace.WithMaxQueueSize(10_000),
		trace.WithMaxExportBatchSize(10_000))

	metricsExporter, err := otlpmetricgrpc.New(
		ctx,
		otlpmetricgrpc.WithEndpoint(env.Info().MetricsTelemetryInfo.Endpoint+":"+env.Info().MetricsTelemetryInfo.EndpointPort),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTemporalitySelector(metric.DefaultTemporalitySelector),
	)
	if err != nil {
		return nil, errors.New("unable to create otlp metrics exporter")
	}

	reader := metric.NewPeriodicReader(
		metricsExporter,
		metric.WithInterval(1*time.Second),
	)

	newResource, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			attribute.String("service.name", "aws-advanced-go-wrapper-integration-tests"),
			attribute.String("service.version", "1.0.0"),
		))
	if err != nil {
		return nil, err
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(newResource),
		trace.WithIDGenerator(xray2.NewIDGenerator()),
	)
	tracerProvider.RegisterSpanProcessor(bsp)

	otel.SetTracerProvider(tracerProvider)

	provider := metric.NewMeterProvider(
		metric.WithReader(reader),
		metric.WithResource(newResource),
	)
	otel.SetMeterProvider(provider)

	xray.SetLogger(xraylog.NewDefaultLogger(os.Stdout, xraylog.LogLevelDebug))
	strategy, err := sampling.NewCentralizedStrategyWithFilePath("../resources/sampling_rules.json")
	if err != nil {
		return nil, err
	}
	err = xray.Configure(
		xray.Config{
			DaemonAddr:       env.Info().XrayTracesTelemetryInfo.Endpoint + ":" + env.Info().XrayTracesTelemetryInfo.EndpointPort,
			SamplingStrategy: strategy,
		},
	)
	if err != nil {
		return nil, err
	}

	err = os.Setenv("AWS_XRAY_NOOP_ID", "FALSE")
	if err != nil {
		return nil, err
	}

	return bsp, nil
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
	EnableAllConnectivity(false)
	return VerifyClusterStatus()
}

func BasicSetupInfoLog(name string) error {
	slog.SetLogLoggerLevel(slog.LevelInfo)
	slog.Info(fmt.Sprintf("Test started: %s.", name))
	EnableAllConnectivity(false)
	return VerifyClusterStatus()
}

func BasicCleanup(name string) {
	awsDriver.ClearCaches()
	slog.Info(fmt.Sprintf("Test finished: %s.", name))
}

func SkipForTestEnvironmentFeatures(t *testing.T, testEnvironmentRequestFeatures []TestEnvironmentFeatures, featuresToSkip ...TestEnvironmentFeatures) {
	for _, featureToSkip := range featuresToSkip {
		if slices.Contains(testEnvironmentRequestFeatures, featureToSkip) {
			t.Skipf("Skipping test for Test Environment Feature: %s", featureToSkip)
			return
		}
	}
}

func RequireTestEnvironmentFeatures(t *testing.T, testEnvironmentRequestFeatures []TestEnvironmentFeatures, requiredFeatures ...TestEnvironmentFeatures) {
	for _, requiredFeature := range requiredFeatures {
		if !slices.Contains(testEnvironmentRequestFeatures, requiredFeature) {
			t.Skipf("Skipping test because required test environment feature was not found: %s", requiredFeature)
			return
		}
	}
}

func (a AuroraTestUtility) GetBlueGreenDeployment(ctx context.Context, blueGreenId string) (*types.BlueGreenDeployment, error) {
	input := &rds.DescribeBlueGreenDeploymentsInput{
		BlueGreenDeploymentIdentifier: aws.String(blueGreenId),
	}

	resp, err := a.client.DescribeBlueGreenDeployments(ctx, input)
	if err != nil {
		slog.Debug("Failed to describe Blue/Green deployment", "blueGreenId", blueGreenId, "error", err)
		return nil, err
	}

	if len(resp.BlueGreenDeployments) == 0 {
		return nil, fmt.Errorf("Blue/Green deployment not found: %s", blueGreenId)
	}

	return &resp.BlueGreenDeployments[0], nil
}

func (a AuroraTestUtility) GetRdsInstanceInfoByArn(ctx context.Context, instanceArn string) (*types.DBInstance, error) {
	input := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(instanceArn),
	}

	resp, err := a.client.DescribeDBInstances(ctx, input)
	if err != nil {
		slog.Debug("Failed to describe RDS instance", "instanceArn", instanceArn, "error", err)
		return nil, err
	}

	if len(resp.DBInstances) == 0 {
		return nil, fmt.Errorf("RDS instance not found: %s", instanceArn)
	}

	return &resp.DBInstances[0], nil
}

func (a AuroraTestUtility) GetClusterByArn(ctx context.Context, clusterArn string) (*types.DBCluster, error) {
	input := &rds.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterArn),
	}

	resp, err := a.client.DescribeDBClusters(ctx, input)
	if err != nil {
		slog.Debug("Failed to describe DB cluster", "clusterArn", clusterArn, "error", err)
		return nil, err
	}

	if len(resp.DBClusters) == 0 {
		return nil, fmt.Errorf("DB cluster not found: %s", clusterArn)
	}

	return &resp.DBClusters[0], nil
}

func (a AuroraTestUtility) GetRdsInstanceIds(engine DatabaseEngine, deployment DatabaseEngineDeployment,
	clusterEndpoint string, port int, dbName, username, password string) ([]string, error) {
	var retrieveTopologySQL string

	switch deployment {
	case AURORA:
		switch engine {
		case MYSQL:
			retrieveTopologySQL = "SELECT SERVER_ID, SESSION_ID FROM information_schema.replica_host_status ORDER BY IF(SESSION_ID = 'MASTER_SESSION_ID', 0, 1)"
		case PG:
			retrieveTopologySQL = "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() ORDER BY CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 0 ELSE 1 END"
		default:
			return nil, fmt.Errorf("unsupported database engine: %v", engine)
		}
	case RDS_MULTI_AZ_CLUSTER:
		switch engine {
		case MYSQL:
			// For Multi-AZ MySQL, we need to get the replica writer ID first
			retrieveTopologySQL = `SELECT SUBSTRING_INDEX(endpoint, '.', 1) as SERVER_ID
								   FROM mysql.rds_topology
								   ORDER BY CASE WHEN id = @@server_id THEN 0 ELSE 1 END,
								            SUBSTRING_INDEX(endpoint, '.', 1)`
		case PG:
			retrieveTopologySQL = `SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as SERVER_ID 
								   FROM rds_tools.show_topology() 
								   ORDER BY CASE WHEN id = (SELECT MAX(multi_az_db_cluster_source_dbi_resource_id) 
								                            FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()) 
								                 THEN 0 ELSE 1 END, endpoint`
		default:
			return nil, fmt.Errorf("unsupported database engine: %v", engine)
		}
	case RDS_MULTI_AZ_INSTANCE:
		switch engine {
		case MYSQL:
			retrieveTopologySQL = "SELECT SUBSTRING_INDEX(endpoint, '.', 1) as SERVER_ID FROM mysql.rds_topology"
		case PG:
			retrieveTopologySQL = "SELECT SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) as SERVER_ID FROM rds_tools.show_topology()"
		default:
			return nil, fmt.Errorf("unsupported database engine: %v", engine)
		}
	default:
		return nil, fmt.Errorf("unsupported database engine deployment: %v", deployment)
	}

	// Create connection to execute the query
	var dsn string
	if engine == PG {
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=require", username, password, clusterEndpoint, port, dbName)
	} else { // MySQL
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, clusterEndpoint, port, dbName)
	}

	db, err := OpenDb(engine, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	rows, err := db.Query(retrieveTopologySQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute topology query: %w", err)
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	var instanceIds []string
	for rows.Next() {
		var serverId string
		var sessionId string // For Aurora queries that return both columns

		// Handle different query result structures
		if deployment == AURORA {
			err = rows.Scan(&serverId, &sessionId)
		} else {
			err = rows.Scan(&serverId)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to scan topology result: %w", err)
		}

		instanceIds = append(instanceIds, serverId)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating topology results: %w", err)
	}

	return instanceIds, nil
}

func (a AuroraTestUtility) SwitchoverBlueGreenDeployment(ctx context.Context, blueGreenId string) error {
	input := &rds.SwitchoverBlueGreenDeploymentInput{
		BlueGreenDeploymentIdentifier: aws.String(blueGreenId),
	}

	resp, err := a.client.SwitchoverBlueGreenDeployment(ctx, input)
	if err != nil {
		slog.Error("Failed to trigger Blue/Green switchover", "blueGreenId", blueGreenId, "error", err)
		return fmt.Errorf("failed to trigger Blue/Green switchover: %w", err)
	}

	if resp.BlueGreenDeployment != nil {
		slog.Debug("Blue/Green switchover request sent successfully", "blueGreenId", blueGreenId)
	}

	return nil
}
