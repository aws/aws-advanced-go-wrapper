/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.host.util;

import integration.host.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.awssdk.services.rds.waiters.RdsWaiter;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static integration.host.DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Creates and destroys AWS RDS Clusters and Instances. To use this functionality the following environment variables
 * must be defined: - AWS_ACCESS_KEY_ID - AWS_SECRET_ACCESS_KEY
 */
public class AuroraTestUtility {

  private static final Logger LOGGER = Logger.getLogger(AuroraTestUtility.class.getName());
  private static final int MULTI_AZ_SIZE = 3;

  // Default values
  private String dbUsername = "my_test_username";
  private String dbPassword = "my_test_password";
  private String dbIdentifier = "test-identifier";
  private String dbEngine = "aurora-postgresql";
  private final String dbSecGroup = "default";
  private static final String DEFAULT_STORAGE_TYPE = "gp3";
  private static final int DEFAULT_IOPS = 64000;
  private static final int DEFAULT_ALLOCATED_STORAGE = 400;
  private final String limitlessDbEngineVersion = "16.4-limitless";
  private String limitlessDbShardGroupIdentifier = "test-db-shard-group-identifier";
  private String monitoringRoleArn = System.getenv("AWS_RDS_MONITORING_ROLE_ARN");
  private final double minAcu = 28.0;
  private final double maxAcu = 601.0;

  private final RdsClient rdsClient;
  private final Ec2Client ec2Client;

  private static final String DUPLICATE_IP_ERROR_CODE = "InvalidPermission.Duplicate";

  public AuroraTestUtility(String region, String endpoint) {
    this(getRegionInternal(region), endpoint, DefaultCredentialsProvider.create());
  }

  public AuroraTestUtility(
      String region, String rdsEndpoint, String awsAccessKeyId, String awsSecretAccessKey, String awsSessionToken) {

    this(
        getRegionInternal(region),
        rdsEndpoint,
        StaticCredentialsProvider.create(
            StringUtils.isNullOrEmpty(awsSessionToken)
                ? AwsBasicCredentials.create(awsAccessKeyId, awsSecretAccessKey)
                : AwsSessionCredentials.create(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)));
  }

  /**
   * Initializes an AmazonRDS & AmazonEC2 client.
   *
   * @param region              define AWS Regions, refer to
   *                            <a
   *                            href="https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html">Regions,
   *                            Availability Zones, and Local Zones</a>
   * @param credentialsProvider Specific AWS credential provider
   */
  public AuroraTestUtility(Region region, String rdsEndpoint, AwsCredentialsProvider credentialsProvider) {
      final RdsClientBuilder rdsClientBuilder = RdsClient.builder()
        .region(region)
        .credentialsProvider(credentialsProvider);

    if (!StringUtils.isNullOrEmpty(rdsEndpoint)) {
      try {
        rdsClientBuilder.endpointOverride(new URI(rdsEndpoint));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    rdsClient = rdsClientBuilder.build();
    ec2Client = Ec2Client.builder()
        .region(region)
        .credentialsProvider(credentialsProvider)
        .build();
  }

  protected static Region getRegionInternal(String rdsRegion) {
    Optional<Region> regionOptional =
        Region.regions().stream().filter(r -> r.id().equalsIgnoreCase(rdsRegion)).findFirst();

    if (regionOptional.isPresent()) {
      return regionOptional.get();
    }
    throw new IllegalArgumentException(String.format("Unknown AWS region '%s'.", rdsRegion));
  }

  /**
   * Creates an RDS cluster based on the passed in details. After the cluster is created, this method will wait
   * until it is available, adds the current IP address to the default security group, and create a database with the
   * given name within the cluster.
   *
   * @param username      the master username for access to the database
   * @param password      the master password for access to the database
   * @param dbName        the database to create within the cluster
   * @param identifier    the cluster identifier
   * @param deployment    the engine deployment to use
   * @param region        the region that the cluster should be created in
   * @param engine        the engine to use, refer to
   *                      <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/rds/model/CreateDbClusterRequest.Builder.html#engine(java.lang.String)">CreateDbClusterRequest.engine</a>
   * @param instanceClass the instance class, refer to
   *                      <a href="https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.Support.html">Supported instance classes</a>
   * @param version       the database engine's version
   * @param numInstances  the number of instances to create for the cluster
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public void createCluster(
      String username,
      String password,
      String dbName,
      String identifier,
      DatabaseEngineDeployment deployment,
      String region,
      String engine,
      String instanceClass,
      String version,
      @Nullable String clusterParameterGroupName,
      int numInstances)
      throws InterruptedException {
    this.dbUsername = username;
    this.dbPassword = password;
    this.dbIdentifier = identifier;
    this.limitlessDbShardGroupIdentifier = identifier;
    this.dbEngine = engine;

      switch (deployment) {
      case AURORA:
        createAuroraCluster(
            username, password, dbName, identifier, region, engine, instanceClass,
            version, clusterParameterGroupName, numInstances);
        break;
      case AURORA_LIMITLESS:
        createAuroraLimitlessCluster();
        break;
      case RDS_MULTI_AZ_CLUSTER:
        if (numInstances != MULTI_AZ_SIZE) {
          throw new RuntimeException(
              "A multi-az cluster with " + numInstances + " instances was requested, but multi-az clusters must have "
                  + MULTI_AZ_SIZE + " instances.");
        }
        createMultiAzCluster(
            username, password, dbName, identifier, region, engine, instanceClass, version);
        break;
      default:
        throw new UnsupportedOperationException(deployment.toString());
    }
  }

  public String createMultiAzInstance(
      String username,
      String password,
      String dbName,
      String identifier,
      DatabaseEngineDeployment deployment,
      String engine,
      String instanceClass,
      String version,
      ArrayList<TestInstanceInfo> instances) {

    if (deployment != RDS_MULTI_AZ_INSTANCE) {
      throw new UnsupportedOperationException(deployment.toString());
    }

    CreateDbInstanceResponse response = rdsClient.createDBInstance(CreateDbInstanceRequest.builder()
        .dbInstanceIdentifier(identifier)
        .publiclyAccessible(true)
        .dbName(dbName)
        .masterUsername(username)
        .masterUserPassword(password)
        .enableIAMDatabaseAuthentication(true)
        .multiAZ(true)
        .engine(engine)
        .engineVersion(version)
        .dbInstanceClass(instanceClass)
        .enablePerformanceInsights(false)
        .backupRetentionPeriod(1)
        .storageEncrypted(true)
        .storageType(DEFAULT_STORAGE_TYPE)
        .allocatedStorage(DEFAULT_ALLOCATED_STORAGE)
        .iops(DEFAULT_IOPS)
        .tags(this.getTag())
        .build());

    // Wait for all instances to be up
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-instance-id").values(identifier).build()),
            (configurationBuilder) -> configurationBuilder.maxAttempts(240).waitTimeout(Duration.ofMinutes(240)));

    if (waiterResponse.matched().exception().isPresent()) {
      deleteMultiAzInstance(identifier, false);
      throw new RuntimeException(
          "Unable to start AWS RDS Instance after waiting for 240 minutes");
    }

    DescribeDbInstancesResponse dbInstancesResult = waiterResponse.matched().response().orElse(null);
    if (dbInstancesResult == null) {
      throw new RuntimeException("Unable to get instance details.");
    }

    final String endpoint = dbInstancesResult.dbInstances().get(0).endpoint().address();
    final String rdsDomainPrefix = endpoint.substring(endpoint.indexOf('.') + 1);

    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      instances.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }

    return rdsDomainPrefix;
  }

  /**
   * Creates an RDS Aurora cluster based on the passed in details. After the cluster is created, this method will wait
   * until it is available, adds the current IP address to the default security group, and create a database with the
   * given name within the cluster.
   *
   * @param username      the master username for access to the database
   * @param password      the master password for access to the database
   * @param dbName        the database to create within the cluster
   * @param identifier    the cluster identifier
   * @param region        the region that the cluster should be created in
   * @param engine        the engine to use, refer to
   *                      <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/rds/model/CreateDbClusterRequest.Builder.html#engine(java.lang.String)">CreateDbClusterRequest.engine</a>
   * @param instanceClass the instance class, refer to
   *                      <a href="https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.Support.html">Supported instance classes</a>
   * @param version       the database engine's version
   * @param numInstances  the number of instances to create for the cluster
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public void createAuroraCluster(
      String username,
      String password,
      String dbName,
      String identifier,
      String region,
      String engine,
      String instanceClass,
      String version,
      @Nullable String clusterParameterGroupName,
      int numInstances)
      throws InterruptedException {
    final CreateDbClusterRequest dbClusterRequest =
        CreateDbClusterRequest.builder()
            .dbClusterIdentifier(identifier)
            .databaseName(dbName)
            .masterUsername(username)
            .masterUserPassword(password)
            .sourceRegion(region)
            .enableIAMDatabaseAuthentication(true)
            .engine(engine)
            .engineVersion(version)
            .storageEncrypted(true)
            .tags(this.getTag())
            .dbClusterParameterGroupName(clusterParameterGroupName)
            .build();

    rdsClient.createDBCluster(dbClusterRequest);

    // Create Instances
    for (int i = 1; i <= numInstances; i++) {
      final String instanceName = identifier + "-" + i;
      rdsClient.createDBInstance(
          CreateDbInstanceRequest.builder()
              .dbClusterIdentifier(identifier)
              .dbInstanceIdentifier(instanceName)
              .dbInstanceClass(instanceClass)
              .engine(engine)
              .engineVersion(version)
              .publiclyAccessible(true)
              .tags(this.getTag())
              .build());
    }

    // Wait for all instances to be up
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-cluster-id").values(identifier).build()),
            (configurationBuilder) -> configurationBuilder.maxAttempts(480).waitTimeout(Duration.ofMinutes(240)));

    if (waiterResponse.matched().exception().isPresent()) {
      deleteCluster(identifier, DatabaseEngineDeployment.AURORA, false);
      throw new InterruptedException(
          "Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
    }
  }

  /**
   * Creates an RDS multi-az cluster based on the passed in details. After the cluster is created, this method will wait
   * until it is available, adds the current IP address to the default security group, and create a database with the
   * given name within the cluster.
   *
   * @param username      the master username for access to the database
   * @param password      the master password for access to the database
   * @param dbName        the database to create within the cluster
   * @param identifier    the cluster identifier
   * @param region        the region that the cluster should be created in
   * @param engine        the engine to use, refer to
   *                      <a href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/rds/model/CreateDbClusterRequest.Builder.html#engine(java.lang.String)">CreateDbClusterRequest.engine</a>
   * @param instanceClass the instance class, refer to
   *                      <a href="https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.Support.html">Supported instance classes</a>
   * @param version       the database engine's version
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public void createMultiAzCluster(String username,
                                   String password,
                                   String dbName,
                                   String identifier,
                                   String region,
                                   String engine,
                                   String instanceClass,
                                   String version)
      throws InterruptedException {
    CreateDbClusterRequest.Builder clusterBuilder =
        CreateDbClusterRequest.builder()
            .dbClusterIdentifier(identifier)
            .publiclyAccessible(true)
            .databaseName(dbName)
            .masterUsername(username)
            .masterUserPassword(password)
            .sourceRegion(region)
            .engine(engine)
            .engineVersion(version)
            .enablePerformanceInsights(false)
            .backupRetentionPeriod(1)
            .storageEncrypted(true)
            .tags(this.getTag())
            .allocatedStorage(DEFAULT_ALLOCATED_STORAGE)
            .dbClusterInstanceClass(instanceClass)
            .storageType(DEFAULT_STORAGE_TYPE)
            .iops(DEFAULT_IOPS);

    rdsClient.createDBCluster(clusterBuilder.build());

    // For multi-AZ deployments, the cluster instances are created automatically. Wait for all instances to be up.
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-cluster-id").values(identifier).build()),
            (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(30)));

    if (waiterResponse.matched().exception().isPresent()) {
      deleteCluster(identifier, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER, false);
      throw new InterruptedException(
          "Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
    }
  }

  /**
   * Creates an RDS instance under the current cluster and waits until it is up.
   *
   * @param instanceClass the desired instance class of the new instance
   * @param instanceId    the desired instance ID of the new instance
   * @return the instance info for the new instance
   * @throws InterruptedException if the new instance is not available within 5 minutes
   */
  public TestInstanceInfo createInstance(String instanceClass, String instanceId) throws InterruptedException {
    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();

    rdsClient.createDBInstance(
        CreateDbInstanceRequest.builder()
            .dbClusterIdentifier(info.getRdsDbName())
            .dbInstanceIdentifier(instanceId)
            .dbInstanceClass(instanceClass)
            .engine(info.getDatabaseEngine())
            .engineVersion(info.getDatabaseEngineVersion())
            .publiclyAccessible(true)
            .tags(this.getTag())
            .build());

    // Wait for the instance to become available
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-instance-id").values(instanceId).build()),
            (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(15)));

    if (waiterResponse.matched().exception().isPresent()) {
      throw new InterruptedException(
          "Instance creation timeout for " + instanceId
              + ". The instance was not available within 5 minutes");
    }

    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(
                    Filter.builder().name("db-instance-id").values(instanceId).build()));

    if (dbInstancesResult.dbInstances().size() != 1) {
      throw new RuntimeException(
          "The describeDBInstances request for newly created instance " + instanceId
              + " returned an unexpected number of instances: "
              + dbInstancesResult.dbInstances().size());
    }

    DBInstance instance = dbInstancesResult.dbInstances().get(0);
    return new TestInstanceInfo(
        instance.dbInstanceIdentifier(),
        instance.endpoint().address(),
        instance.endpoint().port());
  }

  public List<DBInstance> getDBInstances(String clusterId) {
    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(Filter.builder().name("db-cluster-id").values(clusterId).build()));
    return dbInstancesResult.dbInstances();
  }

  /**
   * Deletes an RDS instance.
   *
   * @param instanceToDelete the info for the instance to delete
   * @throws InterruptedException if the instance has not been deleted within 5 minutes
   */
  public void deleteInstance(TestInstanceInfo instanceToDelete) throws InterruptedException {
    rdsClient.deleteDBInstance(
        DeleteDbInstanceRequest.builder()
            .dbInstanceIdentifier(instanceToDelete.getInstanceId())
            .skipFinalSnapshot(true)
            .build());
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse = waiter.waitUntilDBInstanceDeleted(
        (requestBuilder) -> requestBuilder.filters(
            Filter.builder().name("db-instance-id").values(instanceToDelete.getInstanceId())
                .build()),
        (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(15)));

    if (waiterResponse.matched().exception().isPresent()) {
      throw new InterruptedException(
          "Instance deletion timeout for " + instanceToDelete.getInstanceId()
              + ". The instance was not deleted within 5 minutes");
    }
  }

  public void createCustomClusterParameterGroup(
      String groupName, String engine, String engineVersion, DatabaseEngine databaseEngine) {
    CreateDbClusterParameterGroupResponse response = rdsClient.createDBClusterParameterGroup(
        CreateDbClusterParameterGroupRequest.builder()
            .dbClusterParameterGroupName(groupName)
            .description("Test custom cluster parameter group for BGD.")
            .dbParameterGroupFamily(this.getAuroraParameterGroupFamily(engine, engineVersion))
            .build());

    if (!response.sdkHttpResponse().isSuccessful()) {
      throw new RuntimeException("Error creating custom cluster parameter group. " + response.sdkHttpResponse());
    }

    ModifyDbClusterParameterGroupResponse response2;
    switch (databaseEngine) {
      case MYSQL:
        response2 = rdsClient.modifyDBClusterParameterGroup(
            ModifyDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(groupName)
                .parameters(Parameter.builder()
                    .parameterName("binlog_format")
                    .parameterValue("ROW")
                    .applyMethod(ApplyMethod.PENDING_REBOOT)
                    .build())
                .build());
        break;
      case PG:
        response2 = rdsClient.modifyDBClusterParameterGroup(
            ModifyDbClusterParameterGroupRequest.builder()
                .dbClusterParameterGroupName(groupName)
                .parameters(Parameter.builder()
                    .parameterName("rds.logical_replication")
                    .parameterValue("true")
                    .applyMethod(ApplyMethod.PENDING_REBOOT)
                    .build())
                .build());
        break;
      default:
        throw new UnsupportedOperationException(databaseEngine.toString());
    }

    if (!response2.sdkHttpResponse().isSuccessful()) {
      throw new RuntimeException("Error updating parameter. " + response2.sdkHttpResponse());
    }
  }

  public void deleteCustomClusterParameterGroup(String groupName) {
    rdsClient.deleteDBClusterParameterGroup(
        DeleteDbClusterParameterGroupRequest.builder()
            .dbClusterParameterGroupName(groupName)
            .build()
    );
  }

  /**
   * Gets public IP.
   *
   * @return public IP of user
   * @throws UnknownHostException when checkip host isn't available
   */
  public String getPublicIPAddress() throws UnknownHostException {
    String ip;
    try {
      URL ipChecker = new URL("http://checkip.amazonaws.com");
      BufferedReader reader = new BufferedReader(new InputStreamReader(ipChecker.openStream()));
      ip = reader.readLine();
    } catch (Exception e) {
      throw new UnknownHostException("Unable to get IP");
    }
    return ip;
  }

  /**
   * Authorizes IP to EC2 Security groups for RDS access.
   */
  public void ec2AuthorizeIP(String ipAddress) {
    if (StringUtils.isNullOrEmpty(ipAddress)) {
      return;
    }

    if (ipExists(ipAddress)) {
      return;
    }

    try {
      ec2Client.authorizeSecurityGroupIngress(
          (builder) ->
              builder
                  .groupName(dbSecGroup)
                  .ipPermissions((permissionBuilder) ->
                      permissionBuilder.ipRanges((ipRangeBuilder) ->
                              ipRangeBuilder
                                  .cidrIp(ipAddress + "/32")
                                  .description("Test run at " + Instant.now()))
                          .ipProtocol("-1") // All protocols
                          .fromPort(0) // For all ports
                          .toPort(65535)));
    } catch (Ec2Exception exception) {
      if (!DUPLICATE_IP_ERROR_CODE.equalsIgnoreCase(exception.awsErrorDetails().errorCode())) {
        throw exception;
      }
    }
  }

  private boolean ipExists(String ipAddress) {
    final DescribeSecurityGroupsResponse response =
        ec2Client.describeSecurityGroups(
            (builder) ->
                builder
                    .groupNames(dbSecGroup)
                    .filters(
                        software.amazon.awssdk.services.ec2.model.Filter.builder()
                            .name("ip-permission.cidr")
                            .values(ipAddress + "/32")
                            .build()));

    return response != null && !response.securityGroups().isEmpty();
  }

  /**
   * De-authorizes IP from EC2 Security groups.
   */
  public void ec2DeauthorizesIP(String ipAddress) {
    if (StringUtils.isNullOrEmpty(ipAddress)) {
      return;
    }
    try {
      ec2Client.revokeSecurityGroupIngress(
          (builder) ->
              builder
                  .groupName(dbSecGroup)
                  .cidrIp(ipAddress + "/32")
                  .ipProtocol("-1") // All protocols
                  .fromPort(0) // For all ports
                  .toPort(65535));
    } catch (Ec2Exception exception) {
      // Ignore
    }
  }

  /**
   * Deletes the specified cluster and removes the current IP address from the default security group.
   *
   * @param identifier        the cluster identifier for the cluster to delete
   * @param deployment        the engine deployment for the cluster to delete
   * @param waitForCompletion if true, wait for cluster completely deleted
   */
  public void deleteCluster(String identifier, DatabaseEngineDeployment deployment, boolean waitForCompletion) {
    switch (deployment) {
      case AURORA:
        this.deleteAuroraCluster(identifier, waitForCompletion);
        break;
      case RDS_MULTI_AZ_CLUSTER:
        this.deleteMultiAzCluster(identifier, waitForCompletion);
        break;
      case AURORA_LIMITLESS:
        this.deleteAuroraLimitlessCluster();
        break;
      default:
        throw new UnsupportedOperationException(deployment.toString());
    }
  }

  /**
   * Deletes the specified Aurora cluster and removes the current IP address from the default security group.
   *
   * @param identifier        the cluster identifier for the cluster to delete
   * @param waitForCompletion if true, wait for cluster completely deleted
   */
  public void deleteAuroraCluster(String identifier, boolean waitForCompletion) {
    DBCluster dbCluster = getDBCluster(identifier);
    if (dbCluster == null) {
      return;
    }
    List<DBClusterMember> members = dbCluster.dbClusterMembers();

    // Tear down instances
    for (DBClusterMember member : members) {
      try {
        rdsClient.deleteDBInstance(
            DeleteDbInstanceRequest.builder()
                .dbInstanceIdentifier(member.dbInstanceIdentifier())
                .skipFinalSnapshot(true)
                .build());
      } catch (Exception ex) {
        LOGGER.finest("Error deleting instance '"
            + member.dbInstanceIdentifier() + "' of Aurora cluster: " + ex.getMessage());
        // Ignore this error and continue with other instances
      }
    }

    // Tear down cluster
    // Tear down cluster
    int remainingAttempts = 5;
    while (--remainingAttempts > 0) {
      try {
        DeleteDbClusterResponse response = rdsClient.deleteDBCluster(
            (builder -> builder.skipFinalSnapshot(true).dbClusterIdentifier(identifier)));
        if (response.sdkHttpResponse().isSuccessful()) {
          break;
        }
        TimeUnit.SECONDS.sleep(30);

      } catch (DbClusterNotFoundException ex) {
        // ignore
        return;
      } catch (InvalidDbClusterStateException ex) {
        throw new RuntimeException("Error deleting db cluster " + identifier, ex);
      } catch (Exception ex) {
        LOGGER.warning("Error deleting db cluster " + identifier + ": " + ex);
        return;
      }
    }

    if (waitForCompletion) {
      final RdsWaiter waiter = rdsClient.waiter();
      WaiterResponse<DescribeDbClustersResponse> waiterResponse =
          waiter.waitUntilDBClusterDeleted(
              (requestBuilder) ->
                  requestBuilder.filters(
                      Filter.builder().name("db-cluster-id").values(identifier).build()),
              (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(60)));

      if (waiterResponse.matched().exception().isPresent()) {
        throw new RuntimeException(
            "Unable to delete AWS Aurora Cluster after waiting for 60 minutes");
      }
    }
  }

  /**
   * Destroys the limitless cluster. Removes IP from EC2 whitelist.
   *
   */
  public void deleteAuroraLimitlessCluster() {
    int remainingAttempts = 5;

    // Destroy the cluster's shard
    while (--remainingAttempts > 0) {
      try {
        DeleteDbShardGroupResponse response = rdsClient.deleteDBShardGroup(
            (builder -> builder.dbShardGroupIdentifier(limitlessDbShardGroupIdentifier)));
        if (response.sdkHttpResponse().isSuccessful()) {
          break;
        }
        TimeUnit.SECONDS.sleep(30);
      } catch (Exception ex) {
        // ignore
      }
    }

    // Then, destroy the cluster
    deleteDbClusterInternal();
  }

  /**
   * Deletes the specified multi-az cluster and removes the current IP address from the default security group.
   *
   * @param identifier        the cluster identifier for the cluster to delete
   * @param waitForCompletion if true, wait for cluster completely deleted
   */
  public void deleteMultiAzCluster(String identifier, boolean waitForCompletion) {
    // Tear down cluster
    int remainingAttempts = 5;
    while (--remainingAttempts > 0) {
      try {
        DeleteDbClusterResponse response = rdsClient.deleteDBCluster(
            (builder -> builder.skipFinalSnapshot(true).dbClusterIdentifier(identifier)));
        if (response.sdkHttpResponse().isSuccessful()) {
          break;
        }
        TimeUnit.SECONDS.sleep(30);

      } catch (DbClusterNotFoundException ex) {
        // ignore
        return;
      } catch (Exception ex) {
        LOGGER.warning("Error deleting db cluster " + identifier + ": " + ex);
        return;
      }
    }

    if (waitForCompletion) {
      final RdsWaiter waiter = rdsClient.waiter();
      WaiterResponse<DescribeDbClustersResponse> waiterResponse =
          waiter.waitUntilDBClusterDeleted(
              (requestBuilder) ->
                  requestBuilder.filters(
                      Filter.builder().name("db-cluster-id").values(identifier).build()),
              (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(60)));

      if (waiterResponse.matched().exception().isPresent()) {
        throw new RuntimeException(
            "Unable to delete RDS MultiAz Cluster after waiting for 60 minutes");
      }
    }
  }

  public void deleteMultiAzInstance(final String identifier, boolean waitForCompletion) {
    // Tear down MultiAz Instance
    int remainingAttempts = 5;
    while (--remainingAttempts > 0) {
      try {
        DeleteDbInstanceResponse response = rdsClient.deleteDBInstance(
            builder -> builder.skipFinalSnapshot(true).dbInstanceIdentifier(identifier).build());
        if (response.sdkHttpResponse().isSuccessful()) {
          break;
        }
        TimeUnit.SECONDS.sleep(30);

      } catch (InvalidDbInstanceStateException invalidDbInstanceStateException) {
        // Instance is already being deleted.
        // ignore it
        LOGGER.finest("MultiAz Instance " + identifier + " is already being deleted. "
            + invalidDbInstanceStateException);
        break;
      } catch (DbInstanceNotFoundException ex) {
        // ignore
        LOGGER.warning("Error deleting db MultiAz Instance " + identifier + ". Instance not found: " + ex);
        break;
      } catch (Exception ex) {
        LOGGER.warning("Error deleting db MultiAz Instance " + identifier + ": " + ex);
      }
    }

    if (waitForCompletion) {
      final RdsWaiter waiter = rdsClient.waiter();
      WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
          waiter.waitUntilDBInstanceDeleted(
              (requestBuilder) ->
                  requestBuilder.filters(
                      Filter.builder().name("db-instance-id").values(identifier).build()),
              (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(60)));

      if (waiterResponse.matched().exception().isPresent()) {
        throw new RuntimeException(
            "Unable to delete RDS MultiAz Instance after waiting for 60 minutes");
      }
    }
  }

  private void deleteDbClusterInternal() {
    int remainingAttempts = 5;
    while (--remainingAttempts > 0) {
      try {
        DeleteDbClusterResponse response = rdsClient.deleteDBCluster(
            (builder -> builder.skipFinalSnapshot(true).dbClusterIdentifier(dbIdentifier)));
        if (response.sdkHttpResponse().isSuccessful()) {
          break;
        }
        TimeUnit.SECONDS.sleep(30);

      } catch (DbClusterNotFoundException ex) {
        // ignore
      } catch (Exception ex) {
        LOGGER.warning("Error deleting db cluster " + dbIdentifier + ": " + ex);
      }
    }
  }

  public void promoteClusterToStandalone(String clusterArn) {
    if (StringUtils.isNullOrEmpty(clusterArn)) {
      return;
    }

    DBCluster clusterInfo = getClusterByArn(clusterArn);

    if (clusterInfo == null || StringUtils.isNullOrEmpty(clusterInfo.replicationSourceIdentifier())) {
      return;
    }

    PromoteReadReplicaDbClusterResponse response = rdsClient.promoteReadReplicaDBCluster(
        PromoteReadReplicaDbClusterRequest.builder().dbClusterIdentifier(clusterInfo.dbClusterIdentifier()).build());
    if (!response.sdkHttpResponse().isSuccessful()) {
      LOGGER.warning("Error promoting DB cluster to standalone cluster: "
          + response.sdkHttpResponse().statusCode()
          + " "
          + response.sdkHttpResponse().statusText().orElse("<null>"));
    }
  }

  public void promoteInstanceToStandalone(String instanceArn) {
    if (StringUtils.isNullOrEmpty(instanceArn)) {
      return;
    }

    DBInstance instanceInfo = getRdsInstanceInfoByArn(instanceArn);

    if (instanceInfo == null || StringUtils.isNullOrEmpty(instanceInfo.readReplicaSourceDBInstanceIdentifier())) {
      return;
    }

    PromoteReadReplicaResponse response = rdsClient.promoteReadReplica(
        PromoteReadReplicaRequest.builder().dbInstanceIdentifier(instanceInfo.dbInstanceIdentifier()).build());
    if (!response.sdkHttpResponse().isSuccessful()) {
      LOGGER.warning("Error promoting DB instance to standalone instance: "
          + response.sdkHttpResponse().statusCode()
          + " "
          + response.sdkHttpResponse().statusText().orElse("<null>"));
    }
  }

  public boolean doesClusterExist(final String clusterId) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build();
    try {
      rdsClient.describeDBClusters(request);
    } catch (DbClusterNotFoundException ex) {
      return false;
    }
    return true;
  }

  public boolean doesInstanceExist(final String instanceId) {
    final DescribeDbInstancesRequest request =
        DescribeDbInstancesRequest.builder().dbInstanceIdentifier(instanceId).build();
    try {
      DescribeDbInstancesResponse response = rdsClient.describeDBInstances(request);
      return response.sdkHttpResponse().isSuccessful();
    } catch (DbInstanceNotFoundException ex) {
      return false;
    }
  }

  public DBCluster getClusterInfo(final String clusterId) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build();
    final DescribeDbClustersResponse response = rdsClient.describeDBClusters(request);
    if (!response.hasDbClusters()) {
      throw new RuntimeException("Cluster " + clusterId + " not found.");
    }

    return response.dbClusters().get(0);
  }

  public DBCluster getClusterByArn(final String clusterArn) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder()
            .filters(Filter.builder().name("db-cluster-id").values(clusterArn).build())
            .build();
    final DescribeDbClustersResponse response = rdsClient.describeDBClusters(request);
    if (!response.hasDbClusters()) {
      return null;
    }

    return response.dbClusters().get(0);
  }

  public DBInstance getRdsInstanceInfo(final String instanceId) {
    final DescribeDbInstancesRequest request =
        DescribeDbInstancesRequest.builder().dbInstanceIdentifier(instanceId).build();
    final DescribeDbInstancesResponse response = rdsClient.describeDBInstances(request);
    if (!response.hasDbInstances()) {
      throw new RuntimeException("RDS Instance " + instanceId + " not found.");
    }

    return response.dbInstances().get(0);
  }

  public DBInstance getRdsInstanceInfoByArn(final String instanceArn) {
    final DescribeDbInstancesRequest request =
        DescribeDbInstancesRequest.builder().filters(
                Filter.builder().name("db-instance-id").values(instanceArn).build())
            .build();
    final DescribeDbInstancesResponse response = rdsClient.describeDBInstances(request);
    if (!response.hasDbInstances()) {
      return null;
    }

    return response.dbInstances().get(0);
  }

  public DatabaseEngine getClusterEngine(final DBCluster cluster) {
    switch (cluster.engine()) {
      case "aurora-postgresql":
      case "aurora-limitless-postgresql":
      case "postgres":
        return DatabaseEngine.PG;
      case "aurora-mysql":
      case "mysql":
        return DatabaseEngine.MYSQL;
      default:
        throw new UnsupportedOperationException(cluster.engine());
    }
  }

  public static String getDbInstanceClass(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
        return request.getFeatures().contains(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)
            ? "db.r7g.2xlarge"
            : "db.r5.large";
      case RDS:
      case RDS_MULTI_AZ_INSTANCE:
      case RDS_MULTI_AZ_CLUSTER:
        return "db.m5d.large";
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }
  }

  public DatabaseEngine getRdsInstanceEngine(final DBInstance instance) {
    switch (instance.engine()) {
      case "postgres":
        return DatabaseEngine.PG;
      case "mysql":
        return DatabaseEngine.MYSQL;
      default:
        throw new UnsupportedOperationException(instance.engine());
    }
  }

  public String getAuroraParameterGroupFamily(String engine, String engineVersion) {
    switch (engine) {
      case "aurora-postgresql":
        String majorVersion = engineVersion.split("\\.")[0];
        return "aurora-postgresql" + majorVersion;
      case "aurora-mysql":
        return "aurora-mysql8.0";
      default:
        throw new UnsupportedOperationException(engine);
    }
  }

  public List<TestInstanceInfo> getTestInstancesInfo(final String clusterId) {
    List<DBInstance> dbInstances = getDBInstances(clusterId);
    List<TestInstanceInfo> instancesInfo = new ArrayList<>();
    for (DBInstance dbInstance : dbInstances) {
      instancesInfo.add(
          new TestInstanceInfo(
              dbInstance.dbInstanceIdentifier(),
              dbInstance.endpoint().address(),
              dbInstance.endpoint().port()));
    }

    return instancesInfo;
  }

  public void waitUntilClusterHasRightState(String clusterId) throws InterruptedException {
    waitUntilClusterHasRightState(clusterId, "available");
  }

  public void waitUntilClusterHasRightState(String clusterId, String... allowedStatuses) throws InterruptedException {
    String status = getDBCluster(clusterId).status();
    LOGGER.finest("Cluster status: " + status + ", waiting for status: " + String.join(", ", allowedStatuses));
    final Set<String> allowedStatusSet = Arrays.stream(allowedStatuses)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());
    final long waitTillNanoTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(15);
    while (!allowedStatusSet.contains(status.toLowerCase()) && waitTillNanoTime > System.nanoTime()) {
      TimeUnit.MILLISECONDS.sleep(1000);
      String tmpStatus = getDBCluster(clusterId).status();
      if (!tmpStatus.equalsIgnoreCase(status)) {
        LOGGER.finest("Cluster status (waiting): " + tmpStatus);
      }
      status = tmpStatus;
    }
    LOGGER.finest("Cluster status (after wait): " + status);
  }

  public DBCluster getDBCluster(String clusterId) {
    DescribeDbClustersResponse dbClustersResult = null;
    int remainingTries = 5;
    while (remainingTries-- > 0) {
      try {
        dbClustersResult = rdsClient.describeDBClusters((builder) -> builder.dbClusterIdentifier(clusterId));
        break;
      } catch (DbClusterNotFoundException ex) {
        return null;
      } catch (SdkClientException sdkClientException) {
        if (remainingTries == 0) {
          throw sdkClientException;
        }
      }
    }

    if (dbClustersResult == null) {
      fail("Unable to get DB cluster info for cluster with ID " + clusterId);
    }

    final List<DBCluster> dbClusterList = dbClustersResult.dbClusters();
    return dbClusterList.get(0);
  }

  public DBInstance getDBInstance(String instanceId) {
    DescribeDbInstancesResponse dbInstanceResult = null;
    int remainingTries = 5;
    while (remainingTries-- > 0) {
      try {
        dbInstanceResult = rdsClient.describeDBInstances((builder) -> builder.dbInstanceIdentifier(instanceId));
        break;
      } catch (SdkClientException sdkClientException) {
        if (remainingTries == 0) {
          throw sdkClientException;
        }

        try {
          TimeUnit.SECONDS.sleep(30);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ex);
        }
      }
    }

    if (dbInstanceResult == null) {
      fail("Unable to get DB instance info for instance with ID " + instanceId);
    }

    final List<DBInstance> dbClusterList = dbInstanceResult.dbInstances();
    return dbClusterList.get(0);
  }

  public void waitUntilInstanceHasRightState(String instanceId, String... allowedStatuses) throws InterruptedException {

    String status = getDBInstance(instanceId).dbInstanceStatus();
    LOGGER.finest("Instance " + instanceId + " status: " + status
        + ", waiting for status: " + String.join(", ", allowedStatuses));
    final Set<String> allowedStatusSet = Arrays.stream(allowedStatuses)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());
    final long waitTillNanoTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(15);
    while (!allowedStatusSet.contains(status.toLowerCase()) && waitTillNanoTime > System.nanoTime()) {
      TimeUnit.MILLISECONDS.sleep(1000);
      String tmpStatus = getDBInstance(instanceId).dbInstanceStatus();
      if (!tmpStatus.equalsIgnoreCase(status)) {
        LOGGER.finest("Instance " + instanceId + " status (waiting): " + tmpStatus);
      }
      status = tmpStatus;
    }
    LOGGER.finest("Instance " + instanceId + " status (after wait): " + status);
  }

  public List<TestInstanceInfo> getClusterInstanceIds(final String clusterId) {
    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(Filter.builder().name("db-cluster-id").values(clusterId).build()));

    List<TestInstanceInfo> result = new ArrayList<>();
    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      result.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }
    return result;
  }

  public static void registerDriver(DatabaseEngine engine) {
    try {
      Class.forName(DriverHelper.getDriverClassname(engine));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Driver not found: "
              + DriverHelper.getDriverClassname(engine),
          e);
    }
  }

  public void addAuroraAwsIamUser(
      DatabaseEngine databaseEngine,
      String connectionUrl,
      String userName,
      String password,
      String dbUser,
      String databaseName,
      boolean useRdsTools)
      throws SQLException {

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
         final Statement stmt = conn.createStatement()) {

      switch (databaseEngine) {
        case MYSQL:
          stmt.execute("DROP USER IF EXISTS " + dbUser + ";");
          stmt.execute(
              "CREATE USER " + dbUser + " IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';");
          if (!StringUtils.isNullOrEmpty(databaseName)) {
            stmt.execute("GRANT ALL PRIVILEGES ON " + databaseName + ".* TO '" + dbUser + "'@'%';");
          } else {
            stmt.execute("GRANT ALL PRIVILEGES ON `%`.* TO '" + dbUser + "'@'%';");
          }

          // BG switchover status needs it.
          stmt.execute("GRANT SELECT ON mysql.* TO '" + dbUser + "'@'%';");
          break;
        case PG:
          stmt.execute("DROP USER IF EXISTS " + dbUser + ";");
          stmt.execute("CREATE USER " + dbUser + ";");
          stmt.execute("GRANT rds_iam TO " + dbUser + ";");
          if (!StringUtils.isNullOrEmpty(databaseName)) {
            stmt.execute("GRANT ALL PRIVILEGES ON DATABASE " + databaseName + " TO " + dbUser + ";");
          }

          if (useRdsTools) {
            // BG switchover status needs it.
            stmt.execute("GRANT USAGE ON SCHEMA rds_tools TO " + dbUser + ";");
            stmt.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA rds_tools TO " + dbUser + ";");
          }
          break;
        default:
          throw new UnsupportedOperationException(databaseEngine.toString());
      }
    }
  }

  public void createRdsExtension(
      DatabaseEngine databaseEngine,
      String connectionUrl,
      String userName,
      String password) {
    AuroraTestUtility.registerDriver(databaseEngine);

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
         final Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE EXTENSION IF NOT EXISTS rds_tools");
    } catch (SQLException e) {
      throw new RuntimeException("An exception occurred while creating the rds_tools extension.", e);
    }
  }

  public String getDefaultVersion(String engine) {
    final DescribeDbEngineVersionsResponse versions = rdsClient.describeDBEngineVersions(
        DescribeDbEngineVersionsRequest.builder()
            .defaultOnly(true)
            .engine(engine)
            .build()
    );
    if (!versions.dbEngineVersions().isEmpty()) {
      return versions.dbEngineVersions().get(0).engineVersion();
    }
    throw new RuntimeException("Failed to find default version");
  }

  public String getLatestVersion(String engine) {
    return getEngineVersions(engine).stream()
        .sorted(Comparator.reverseOrder())
        .findFirst()
        .orElse(null);
  }

  private List<String> getEngineVersions(String engine) {
    final List<String> res = new ArrayList<>();
    final DescribeDbEngineVersionsResponse versions = rdsClient.describeDBEngineVersions(
        DescribeDbEngineVersionsRequest.builder().engine(engine).build()
    );
    for (DBEngineVersion version : versions.dbEngineVersions()) {
      if (version.engineVersion().contains("limitless")) {
        // Skip limitless engine version until limitless support is complete.
        continue;
      }
      res.add(version.engineVersion());
    }
    return res;
  }

  /**
   *
   *
   * @return
   * @throws InterruptedException
   */
  public String createAuroraLimitlessCluster() throws InterruptedException {
    final Tag testRunnerTag = Tag.builder().key("env").value("test-runner").build();

    final CreateDbClusterRequest dbClusterRequest =
        CreateDbClusterRequest.builder()
            .clusterScalabilityType(ClusterScalabilityType.LIMITLESS)
            .databaseName(null)
            .dbClusterIdentifier(dbIdentifier)
            .enableCloudwatchLogsExports("postgresql")
            .enableIAMDatabaseAuthentication(true)
            .enablePerformanceInsights(true)
            .engine(dbEngine)
            .engineVersion(limitlessDbEngineVersion)
            .masterUsername(dbUsername)
            .masterUserPassword(dbPassword)
            .monitoringInterval(5)
            .monitoringRoleArn(monitoringRoleArn)
            .performanceInsightsRetentionPeriod(31)
            .storageType("aurora-iopt1")
            .tags(testRunnerTag)
            .build();
    rdsClient.createDBCluster(dbClusterRequest);

    final CreateDbShardGroupRequest shardGroupRequest =
        CreateDbShardGroupRequest.builder()
            .dbClusterIdentifier(dbIdentifier)
            .dbShardGroupIdentifier(limitlessDbShardGroupIdentifier)
            .minACU(minAcu)
            .maxACU(maxAcu)
            .publiclyAccessible(true)
            .build();
    rdsClient.createDBShardGroup(shardGroupRequest);

    DescribeDbClustersRequest describeDbClustersRequest = DescribeDbClustersRequest.builder()
        .dbClusterIdentifier(dbIdentifier)
        .build();

    WaiterOverrideConfiguration waiterConfig = WaiterOverrideConfiguration.builder()
        .maxAttempts(90)
        .waitTimeout(Duration.ofMinutes(90))
        .build();
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbClustersResponse> waiterResponse = waiter.waitUntilDBClusterAvailable(describeDbClustersRequest, waiterConfig);

    if (waiterResponse.matched().exception().isPresent()) {
      deleteAuroraLimitlessCluster();
      throw new InterruptedException(
          "Unable to start AWS RDS Cluster & Instances after waiting for 90 minutes");
    }

    final DescribeDbShardGroupsResponse dbShardGroupsResponse = rdsClient.describeDBShardGroups(
        (builder) ->
            builder.filters(
                Filter.builder().name("db-cluster-id").values(dbIdentifier).build()));

    final String endpoint = dbShardGroupsResponse.dbShardGroups().get(0).endpoint();
    final String clusterDomainSuffix = endpoint.substring(endpoint.indexOf("shardgrp-") + 9);

    return clusterDomainSuffix;
  }


  public String createBlueGreenDeployment(String name, String sourceArn) {

    final String blueGreenName = "bgd-" + name;

    CreateBlueGreenDeploymentResponse response = null;
    int count = 10;
    while (response == null && count-- > 0) {
      try {
        response = rdsClient.createBlueGreenDeployment(
            CreateBlueGreenDeploymentRequest.builder()
                .blueGreenDeploymentName(blueGreenName)
                .source(sourceArn)
                .tags(this.getTag())
                .build());
      } catch (RdsException ex) {
        if (ex.statusCode() != 500 || count == 0) {
          throw ex;
        }

        LOGGER.finest("Can't send createBlueGreenDeployment request. Wait 1min and try again.");

        try {
          TimeUnit.MINUTES.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }

    if (response == null) {
      throw new RuntimeException("Can't send createBlueGreenDeployment request.");
    }

    if (!response.sdkHttpResponse().isSuccessful()) {
      LOGGER.finest(String.format("createBlueGreenDeployment response: %d, %s",
          response.sdkHttpResponse().statusCode(),
          response.sdkHttpResponse().statusText()));
      throw new RuntimeException(response.sdkHttpResponse().statusText().orElse("Unspecified error."));
    } else {
      LOGGER.finest("createBlueGreenDeployment request is sent");
    }

    String blueGreenId = response.blueGreenDeployment().blueGreenDeploymentIdentifier();

    BlueGreenDeployment blueGreenDeployment = getBlueGreenDeployment(blueGreenId);
    long end = System.nanoTime() + TimeUnit.MINUTES.toNanos(240);
    while ((blueGreenDeployment == null || !blueGreenDeployment.status().equalsIgnoreCase("available"))
        && System.nanoTime() < end) {
      try {
        TimeUnit.SECONDS.sleep(60);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      blueGreenDeployment = getBlueGreenDeployment(blueGreenId);
    }

    if (blueGreenDeployment == null || !blueGreenDeployment.status().equalsIgnoreCase("available")) {
      throw new RuntimeException("BlueGreen Deployment " + blueGreenId + " isn't available.");
    }

    return blueGreenId;
  }

  public void waitUntilBlueGreenDeploymentHasRightState(String blueGreenId, String... allowedStatuses) {

    String status = getBlueGreenDeployment(blueGreenId).status();
    LOGGER.finest("BGD status: " + status + ", waiting for status: " + String.join(", ", allowedStatuses));
    final Set<String> allowedStatusSet = Arrays.stream(allowedStatuses)
        .map(String::toLowerCase)
        .collect(Collectors.toSet());
    final long waitTillNanoTime = System.nanoTime() + TimeUnit.MINUTES.toNanos(15);
    while (!allowedStatusSet.contains(status.toLowerCase()) && waitTillNanoTime > System.nanoTime()) {
      try {
        TimeUnit.MILLISECONDS.sleep(1000);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
      String tmpStatus = getBlueGreenDeployment(blueGreenId).status();
      if (!tmpStatus.equalsIgnoreCase(status)) {
        LOGGER.finest("BGD status (waiting): " + tmpStatus);
      }
      status = tmpStatus;
    }
    LOGGER.finest("BGD status (after wait): " + status);

    if (!allowedStatusSet.contains(status.toLowerCase())) {
      throw new RuntimeException("BlueGreen Deployment " + blueGreenId + " has wrong status.");
    }
  }

  public void switchoverBlueGreenDeployment(String blueGreenId) {
    SwitchoverBlueGreenDeploymentResponse response = rdsClient.switchoverBlueGreenDeployment(
        SwitchoverBlueGreenDeploymentRequest.builder()
            .blueGreenDeploymentIdentifier(blueGreenId)
            .build());

    if (!response.sdkHttpResponse().isSuccessful()) {
      LOGGER.finest(String.format("switchoverBlueGreenDeployment response: %d, %s",
          response.sdkHttpResponse().statusCode(),
          response.sdkHttpResponse().statusText()));
      throw new RuntimeException(response.sdkHttpResponse().statusText().orElse("Unspecified error."));
    } else {
      LOGGER.finest("switchoverBlueGreenDeployment request is sent");
    }
  }

  public boolean doesBlueGreenDeploymentExist(String blueGreenId) {
    try {
      DescribeBlueGreenDeploymentsResponse response = rdsClient.describeBlueGreenDeployments(
          builder -> builder.blueGreenDeploymentIdentifier(blueGreenId));
      return response.blueGreenDeployments() != null && !response.blueGreenDeployments().isEmpty();
    } catch (BlueGreenDeploymentNotFoundException ex) {
      LOGGER.finest("blueGreenDeployments not found");
      return false;
    }
  }

  public BlueGreenDeployment getBlueGreenDeployment(String blueGreenId) {
    try {
      DescribeBlueGreenDeploymentsResponse response = rdsClient.describeBlueGreenDeployments(
          builder -> builder.blueGreenDeploymentIdentifier(blueGreenId));
      if (response.hasBlueGreenDeployments()) {
        return response.blueGreenDeployments().get(0);
      }
      return null;
    } catch (BlueGreenDeploymentNotFoundException ex) {
      return null;
    }
  }

  public BlueGreenDeployment getBlueGreenDeploymentBySource(String sourceArn) {
    try {
      DescribeBlueGreenDeploymentsResponse response = rdsClient.describeBlueGreenDeployments(
          builder -> builder.filters(f -> f.name("source").values(sourceArn)));
      if (!response.blueGreenDeployments().isEmpty()) {
        return response.blueGreenDeployments().get(0);
      }
      return null;
    } catch (BlueGreenDeploymentNotFoundException ex) {
      return null;
    }
  }

  public void deleteBlueGreenDeployment(String blueGreenId, boolean waitForCompletion) {

    if (!doesBlueGreenDeploymentExist(blueGreenId)) {
      return;
    }

    waitUntilBlueGreenDeploymentHasRightState(blueGreenId, "available", "switchover_completed");

    DeleteBlueGreenDeploymentResponse response = rdsClient.deleteBlueGreenDeployment(
        DeleteBlueGreenDeploymentRequest.builder()
            .blueGreenDeploymentIdentifier(blueGreenId)
            .build());

    if (!response.sdkHttpResponse().isSuccessful()) {
      LOGGER.finest(String.format("deleteBlueGreenDeployment response: %d, %s",
          response.sdkHttpResponse().statusCode(),
          response.sdkHttpResponse().statusText()));
      throw new RuntimeException(response.sdkHttpResponse().statusText().orElse("Unspecified error."));
    } else {
      LOGGER.finest("deleteBlueGreenDeployment request is sent");
    }

    if (waitForCompletion) {
      long endTimeNano = System.nanoTime() + TimeUnit.MINUTES.toNanos(120);
      while (doesBlueGreenDeploymentExist(blueGreenId) && endTimeNano > System.nanoTime()) {
        try {
          TimeUnit.MINUTES.sleep(1);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          return;
        }
      }

      if (doesBlueGreenDeploymentExist(blueGreenId)) {
        throw new RuntimeException(
            "Unable to delete Blue/Green Deployment after waiting for 120 minutes");
      }
    }
  }

  private Tag getTag() {
    ZoneId zoneId = ZoneId.of("America/Los_Angeles");
    ZonedDateTime zdt = Instant.now().atZone(zoneId);
    String timeStr = zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss zzz"));
    return Tag.builder()
        .key("env").value("test-runner")
        .key("created").value(timeStr)
        .build();
  }
}
