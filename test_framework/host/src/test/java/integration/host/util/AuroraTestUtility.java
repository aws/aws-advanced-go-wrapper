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

import integration.host.DatabaseEngine;
import integration.host.DatabaseEngineDeployment;
import integration.host.DriverHelper;
import integration.host.TestEnvironment;
import integration.host.TestEnvironmentInfo;
import integration.host.TestInstanceInfo;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.RdsClientBuilder;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.awssdk.services.rds.waiters.RdsWaiter;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Creates and destroys AWS RDS Clusters and Instances. To use this functionality the following environment variables
 * must be defined: - AWS_ACCESS_KEY_ID - AWS_SECRET_ACCESS_KEY
 */
public class AuroraTestUtility {

  private static final Logger LOGGER = Logger.getLogger(AuroraTestUtility.class.getName());

  // Default values
  private String dbUsername = "my_test_username";
  private String dbPassword = "my_test_password";
  private String dbName = "test";
  private String dbIdentifier = "test-identifier";
  private DatabaseEngineDeployment dbEngineDeployment;
  private String dbEngine = "aurora-postgresql";
  private String dbEngineVersion = "13.9";
  private String dbInstanceClass = "db.r5.large";
  private String storageType = "io1";
  private int allocatedStorage = 100;
  private int iops = 1000;
  private final Region dbRegion;
  private final String dbSecGroup = "default";
  private int numOfInstances = 5;
  private List<TestInstanceInfo> instances = new ArrayList<>();

  private final RdsClient rdsClient;
  private final Ec2Client ec2Client;
  private static final Random rand = new Random();

  private static final String DUPLICATE_IP_ERROR_CODE = "InvalidPermission.Duplicate";

  public AuroraTestUtility(String region, String endpoint) throws URISyntaxException {
    this(getRegionInternal(region), endpoint, DefaultCredentialsProvider.create());
  }

  public AuroraTestUtility(
      String region, String rdsEndpoint, String awsAccessKeyId, String awsSecretAccessKey, String awsSessionToken)
      throws URISyntaxException {

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
  public AuroraTestUtility(Region region, String rdsEndpoint, AwsCredentialsProvider credentialsProvider)
      throws URISyntaxException {
    dbRegion = region;
    final RdsClientBuilder rdsClientBuilder = RdsClient.builder()
        .region(dbRegion)
        .credentialsProvider(credentialsProvider);

    if (!StringUtils.isNullOrEmpty(rdsEndpoint)) {
      rdsClientBuilder.endpointOverride(new URI(rdsEndpoint));
    }

    rdsClient = rdsClientBuilder.build();
    ec2Client = Ec2Client.builder()
        .region(dbRegion)
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
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @param username      Master username for access to database
   * @param password      Master password for access to database
   * @param dbName        Database name
   * @param identifier    Database cluster identifier
   * @param engine        Database engine to use, refer to
   *                      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Welcome.html
   * @param instanceClass instance class, refer to
   *                      https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.html
   * @param version       the database engine's version
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createCluster(
      String username,
      String password,
      String dbName,
      String identifier,
      DatabaseEngineDeployment deployment,
      String engine,
      String instanceClass,
      String version,
      int numOfInstances,
      ArrayList<TestInstanceInfo> instances)
      throws InterruptedException {
    this.dbUsername = username;
    this.dbPassword = password;
    this.dbName = dbName;
    this.dbIdentifier = identifier;
    this.dbEngineDeployment = deployment;
    this.dbEngine = engine;
    this.dbInstanceClass = instanceClass;
    this.dbEngineVersion = version;
    this.numOfInstances = numOfInstances;
    this.instances = instances;

    switch (this.dbEngineDeployment) {
      case AURORA:
        return createAuroraCluster();
      case RDS_MULTI_AZ_CLUSTER:
        return createMultiAzCluster();
      default:
        throw new UnsupportedOperationException(this.dbEngineDeployment.toString());
    }
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createAuroraCluster() throws InterruptedException {
    // Create Cluster
    final Tag testRunnerTag = Tag.builder().key("env").value("test-runner").build();

    final CreateDbClusterRequest dbClusterRequest =
        CreateDbClusterRequest.builder()
            .dbClusterIdentifier(dbIdentifier)
            .databaseName(dbName)
            .masterUsername(dbUsername)
            .masterUserPassword(dbPassword)
            .sourceRegion(dbRegion.id())
            .enableIAMDatabaseAuthentication(true)
            .engine(dbEngine)
            .engineVersion(dbEngineVersion)
            .storageEncrypted(true)
            .tags(testRunnerTag)
            .build();

    rdsClient.createDBCluster(dbClusterRequest);

    // Create Instances
    for (int i = 1; i <= numOfInstances; i++) {
      final String instanceName = dbIdentifier + "-" + i;
      rdsClient.createDBInstance(
          CreateDbInstanceRequest.builder()
              .dbClusterIdentifier(dbIdentifier)
              .dbInstanceIdentifier(instanceName)
              .dbInstanceClass(dbInstanceClass)
              .engine(dbEngine)
              .engineVersion(dbEngineVersion)
              .publiclyAccessible(true)
              .tags(testRunnerTag)
              .build());
    }

    // Wait for all instances to be up
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()),
            (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(30)));

    if (waiterResponse.matched().exception().isPresent()) {
      deleteCluster();
      throw new InterruptedException(
          "Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
    }

    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()));
    final String endpoint = dbInstancesResult.dbInstances().get(0).endpoint().address();
    final String clusterDomainSuffix = endpoint.substring(endpoint.indexOf('.') + 1);

    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      this.instances.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }

    return clusterDomainSuffix;
  }

  /**
   * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
   *
   * @return An endpoint for one of the instances
   * @throws InterruptedException when clusters have not started after 30 minutes
   */
  public String createMultiAzCluster() throws InterruptedException {
    // Create Cluster
    final Tag testRunnerTag = Tag.builder().key("env").value("test-runner").build();
    CreateDbClusterRequest.Builder clusterBuilder =
        CreateDbClusterRequest.builder()
            .dbClusterIdentifier(dbIdentifier)
            .publiclyAccessible(true)
            .databaseName(dbName)
            .masterUsername(dbUsername)
            .masterUserPassword(dbPassword)
            .sourceRegion(dbRegion.id())
            .engine(dbEngine)
            .engineVersion(dbEngineVersion)
            .enablePerformanceInsights(false)
            .backupRetentionPeriod(1)
            .storageEncrypted(true)
            .tags(testRunnerTag);

    clusterBuilder =
        clusterBuilder.allocatedStorage(allocatedStorage)
            .dbClusterInstanceClass(dbInstanceClass)
            .storageType(storageType)
            .iops(iops);

    rdsClient.createDBCluster(clusterBuilder.build());

    // For multi-AZ deployments, the cluster instances are created automatically.

    // Wait for all instances to be up
    final RdsWaiter waiter = rdsClient.waiter();
    WaiterResponse<DescribeDbInstancesResponse> waiterResponse =
        waiter.waitUntilDBInstanceAvailable(
            (requestBuilder) ->
                requestBuilder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()),
            (configurationBuilder) -> configurationBuilder.waitTimeout(Duration.ofMinutes(30)));

    if (waiterResponse.matched().exception().isPresent()) {
      deleteCluster();
      throw new InterruptedException(
          "Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
    }

    final DescribeDbInstancesResponse dbInstancesResult =
        rdsClient.describeDBInstances(
            (builder) ->
                builder.filters(
                    Filter.builder().name("db-cluster-id").values(dbIdentifier).build()));
    final String endpoint = dbInstancesResult.dbInstances().get(0).endpoint().address();
    final String clusterDomainSuffix = endpoint.substring(endpoint.indexOf('.') + 1);

    for (DBInstance instance : dbInstancesResult.dbInstances()) {
      this.instances.add(
          new TestInstanceInfo(
              instance.dbInstanceIdentifier(),
              instance.endpoint().address(),
              instance.endpoint().port()));
    }

    return clusterDomainSuffix;
  }

  /**
   * Creates an RDS instance under the current cluster and waits until it is up.
   *
   * @param instanceId the desired instance ID of the new instance
   * @return the instance info of the new instance
   * @throws InterruptedException if the new instance is not available within 5 minutes
   */
  public TestInstanceInfo createInstance(String instanceId) throws InterruptedException {
    final Tag testRunnerTag = Tag.builder().key("env").value("test-runner").build();
    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();

    rdsClient.createDBInstance(
        CreateDbInstanceRequest.builder()
            .dbClusterIdentifier(info.getAuroraClusterName())
            .dbInstanceIdentifier(instanceId)
            .dbInstanceClass(dbInstanceClass)
            .engine(info.getDatabaseEngine())
            .engineVersion(info.getDatabaseEngineVersion())
            .publiclyAccessible(true)
            .tags(testRunnerTag)
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
    TestInstanceInfo instanceInfo = new TestInstanceInfo(
        instance.dbInstanceIdentifier(),
        instance.endpoint().address(),
        instance.endpoint().port());
    this.instances.add(instanceInfo);
    return instanceInfo;
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
    this.instances.remove(instanceToDelete);

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
   * Destroys all instances and clusters. Removes IP from EC2 whitelist.
   *
   * @param identifier database identifier to delete
   */
  public void deleteCluster(String identifier) {
    dbIdentifier = identifier;
    deleteCluster();
  }

  /**
   * Destroys all instances and clusters. Removes IP from EC2 whitelist.
   */
  public void deleteCluster() {

    switch (this.dbEngineDeployment) {
      case AURORA:
        this.deleteAuroraCluster();
        break;
      case RDS_MULTI_AZ_CLUSTER:
        this.deleteMultiAzCluster();
        break;
      default:
        throw new UnsupportedOperationException(this.dbEngineDeployment.toString());
    }
  }

  /**
   * Destroys all instances and clusters. Removes IP from EC2 whitelist.
   */
  public void deleteAuroraCluster() {
    // Tear down instances
    for (int i = 1; i <= numOfInstances; i++) {
      try {
        rdsClient.deleteDBInstance(
            DeleteDbInstanceRequest.builder()
                .dbInstanceIdentifier(dbIdentifier + "-" + i)
                .skipFinalSnapshot(true)
                .build());
      } catch (Exception ex) {
        LOGGER.finest("Error deleting instance " + dbIdentifier + "-" + i + ". " + ex.getMessage());
        // Ignore this error and continue with other instances
      }
    }

    // Tear down cluster
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

  /**
   * Destroys all instances and clusters.
   */
  public void deleteMultiAzCluster() {
    // deleteDBinstance requests are not necessary to delete a multi-az cluster.
    // Tear down cluster
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

  public DBCluster getClusterInfo(final String clusterId) {
    final DescribeDbClustersRequest request =
        DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build();
    final DescribeDbClustersResponse response = rdsClient.describeDBClusters(request);
    if (!response.hasDbClusters()) {
      throw new RuntimeException("Cluster " + clusterId + " not found.");
    }

    return response.dbClusters().get(0);
  }

  public DatabaseEngine getClusterEngine(final DBCluster cluster) {
    switch (cluster.engine()) {
      case "aurora-postgresql":
      case "postgres":
        return DatabaseEngine.PG;
      case "aurora-mysql":
      case "mysql":
        return DatabaseEngine.MYSQL;
      default:
        throw new UnsupportedOperationException(cluster.engine());
    }
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
      String databaseName)
      throws SQLException {
    AuroraTestUtility.registerDriver(databaseEngine);

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
         final Statement stmt = conn.createStatement()) {

      switch (databaseEngine) {
        case MYSQL:
          stmt.execute("DROP USER IF EXISTS " + dbUser + ";");
          stmt.execute(
              "CREATE USER " + dbUser + " IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';");
          stmt.execute("GRANT ALL PRIVILEGES ON " + databaseName + ".* TO '" + dbUser + "'@'%';");
          break;
        case PG:
          stmt.execute("DROP USER IF EXISTS " + dbUser + ";");
          stmt.execute("CREATE USER " + dbUser + ";");
          stmt.execute("GRANT rds_iam TO " + dbUser + ";");
          stmt.execute("GRANT ALL PRIVILEGES ON DATABASE " + databaseName + " TO " + dbUser + ";");
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
      String password)
      throws SQLException {
    AuroraTestUtility.registerDriver(databaseEngine);

    try (final Connection conn = DriverManager.getConnection(connectionUrl, userName, password);
         final Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE EXTENSION IF NOT EXISTS rds_tools");
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
}
