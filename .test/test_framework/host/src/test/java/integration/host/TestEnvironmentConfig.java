package integration.host;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import integration.host.util.AuroraTestUtility;
import integration.host.util.ContainerHelper;
import integration.host.util.StringUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class TestEnvironmentConfig implements AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(TestEnvironmentConfig.class.getName());

  private static final String DATABASE_CONTAINER_NAME_PREFIX = "database-container-";
  private static final String TEST_CONTAINER_NAME = "test-container";
  private static final String TELEMETRY_XRAY_CONTAINER_NAME = "xray-daemon";
  private static final String TELEMETRY_OTLP_CONTAINER_NAME = "otlp-daemon";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  protected static final int PROXY_CONTROL_PORT = 8474;
  private final TestEnvironmentInfo info =
      new TestEnvironmentInfo(); // only this info is passed to test container

  private static final AtomicInteger ipAddressUsageRefCount = new AtomicInteger(0);

  // The following variables are local to host portion of test environment. They are not shared with a
  // test container.

  private int numOfInstances;
  private boolean reuseDb;
  private String rdsDbName; // "cluster-mysql", "instance-name", "rds-multi-az-cluster-name"
  private String rdsDbDomain; // "XYZ.us-west-2.rds.amazonaws.com"
  private String rdsEndpoint; // "https://rds-int.amazon.com"
  public String rdsDbRegion;

  // Expected values: "latest", "default", or engine version, for example, "15.4"
  // If left as empty, will use default version
  public String auroraMySqlDbEngineVersion;
  public String auroraPgDbEngineVersion;

  // Expected values: "latest", "default", or engine version, for example, "15.4"
  // If left as empty, will use default version
  public String rdsMySqlDbEngineVersion;
  public String rdsPgDbEngineVersion;

  private String awsAccessKeyId;
  private String awsSecretAccessKey;
  private String awsSessionToken;

  private GenericContainer<?> testContainer;
  private final ArrayList<GenericContainer<?>> databaseContainers = new ArrayList<>();
  private ArrayList<ToxiproxyContainer> proxyContainers;
  private GenericContainer<?> telemetryXRayContainer;
  private GenericContainer<?> telemetryOtlpContainer;

  private String runnerIP;

  private final Network network = Network.newNetwork();

  private AuroraTestUtility auroraUtil;

  private TestEnvironmentConfig(TestEnvironmentRequest request) {
    this.info.setRequest(request);
  }

  public static TestEnvironmentConfig build(TestEnvironmentRequest request) {
    TestEnvironmentConfig env = new TestEnvironmentConfig(request);

    switch (request.getDatabaseEngineDeployment()) {
      case DOCKER:
        initDatabaseParams(env);
        createDatabaseContainers(env);

        if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
          throw new UnsupportedOperationException(TestEnvironmentFeatures.IAM.toString());
        }

        if (request.getFeatures().contains(TestEnvironmentFeatures.FAILOVER_SUPPORTED)) {
          throw new UnsupportedOperationException(
              TestEnvironmentFeatures.FAILOVER_SUPPORTED.toString());
        }

        if (request.getFeatures().contains(TestEnvironmentFeatures.LIMITLESS_DEPLOYMENT)) {
          throw new UnsupportedOperationException(
              TestEnvironmentFeatures.LIMITLESS_DEPLOYMENT.toString());
        }

        if (request.getFeatures().contains(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)) {
          throw new UnsupportedOperationException(
              TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT.toString());
        }

        break;
      case AURORA:
      case RDS_MULTI_AZ_CLUSTER:
      case RDS_MULTI_AZ_INSTANCE:
        createAuroraOrMultiAzEnvironment(env);

        if (request.getFeatures().contains(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)) {
          createBlueGreenDeployment(env);
        }

        break;
      case AURORA_LIMITLESS:
        initLimitlessDatabaseParams(env);
        initAwsCredentials(env);
        initEnv(env);
        authorizeRunnerIpAddress(env);
        createDbCluster(env);
        if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
          configureIamAccess(env);
        }
        break;
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }

    if (request.getFeatures().contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
      createProxyContainers(env);
    }

    if (request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED)) {
      createTelemetryXRayContainer(env);
    }

    if (request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED)
        || request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED)) {
      createTelemetryOtlpContainer(env);
    }

    createTestContainer(env);

    return env;
  }

  private static void authorizeRunnerIpAddress(TestEnvironmentConfig env) {
    DatabaseEngineDeployment deployment = env.info.getRequest().getDatabaseEngineDeployment();
    if (deployment == DatabaseEngineDeployment.AURORA
        || deployment == DatabaseEngineDeployment.RDS
        || deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE
        || deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER
        || deployment == DatabaseEngineDeployment.AURORA_LIMITLESS) {
      // These environment require creating external database cluster that should be publicly available.
      // Corresponding AWS Security Groups should be configured and the test task runner IP address
      // should be whitelisted.

      if (env.info.getRequest().getFeatures().contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
        if (ipAddressUsageRefCount.incrementAndGet() == 1) {
          authorizeIP(env);
        } else {
          LOGGER.finest("IP usage count: " + ipAddressUsageRefCount.get());
        }
      }
    }
  }

  private static void createAuroraOrMultiAzEnvironment(TestEnvironmentConfig env) {
    initRandomBase(env);
    initDatabaseParams(env);
    initAwsCredentials(env);

    final TestEnvironmentRequest request = env.info.getRequest();
    switch (request.getDatabaseEngineDeployment()) {
      case RDS_MULTI_AZ_INSTANCE:
        initEnv(env);
        authorizeRunnerIpAddress(env);
        createMultiAzInstance(env);
        configureIamAccess(env);
        break;
      case RDS_MULTI_AZ_CLUSTER:
        initEnv(env);
        authorizeRunnerIpAddress(env);
        createDbCluster(env);
        configureIamAccess(env);
        break;
      case AURORA:
        initEnv(env);
        authorizeRunnerIpAddress(env);

        if (!env.reuseDb
            && env.info.getRequest().getFeatures().contains(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)) {
          createCustomClusterParameterGroup(env);
        }
        createDbCluster(env);
        configureIamAccess(env);
        break;
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }

  }

  private static void createBlueGreenDeployment(TestEnvironmentConfig env) {

    if (env.info.getRequest().getDatabaseEngineDeployment() == DatabaseEngineDeployment.AURORA) {
      DBCluster clusterInfo = env.auroraUtil.getClusterInfo(env.rdsDbName);
      if (env.reuseDb) {
        BlueGreenDeployment bgDeployment = env.auroraUtil.getBlueGreenDeploymentBySource(clusterInfo.dbClusterArn());
        if (bgDeployment != null) {
          env.info.getDatabaseInfo().setBlueGreenDeploymentId(bgDeployment.blueGreenDeploymentIdentifier());
          waitForBlueGreenClustersHaveRightState(env, bgDeployment);
          return;
        }
      }

      // Otherwise, create a new BG deployment
      final String blueGreenId = env.auroraUtil.createBlueGreenDeployment(
          env.rdsDbName, clusterInfo.dbClusterArn());
      env.info.getDatabaseInfo().setBlueGreenDeploymentId(blueGreenId);

      BlueGreenDeployment bgDeployment = env.auroraUtil.getBlueGreenDeployment(blueGreenId);
      if (bgDeployment != null) {
        waitForBlueGreenClustersHaveRightState(env, bgDeployment);
      }

    } else if (env.info.getRequest().getDatabaseEngineDeployment() == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE) {
      DBInstance instanceInfo = env.auroraUtil.getRdsInstanceInfo(env.rdsDbName);
      if (env.reuseDb) {
        BlueGreenDeployment bgDeployment = env.auroraUtil.getBlueGreenDeploymentBySource(instanceInfo.dbInstanceArn());
        if (bgDeployment != null) {
          env.info.getDatabaseInfo().setBlueGreenDeploymentId(bgDeployment.blueGreenDeploymentIdentifier());
          waitForBlueGreenInstancesHaveRightState(env, bgDeployment);
          return;
        }
      }

      // Otherwise, create a new BG deployment
      final String blueGreenId = env.auroraUtil.createBlueGreenDeployment(
          env.rdsDbName, instanceInfo.dbInstanceArn());
      env.info.getDatabaseInfo().setBlueGreenDeploymentId(blueGreenId);

      BlueGreenDeployment bgDeployment = env.auroraUtil.getBlueGreenDeployment(blueGreenId);
      if (bgDeployment != null) {
        waitForBlueGreenInstancesHaveRightState(env, bgDeployment);
      }

    } else {
      LOGGER.warning("BG Deployments are supported for RDS MultiAz Instances and Aurora clusters only."
          + " Proceed without creating BG Deployment.");
    }
  }

  private static void waitForBlueGreenClustersHaveRightState(TestEnvironmentConfig env, BlueGreenDeployment bgDeployment) {
    DBCluster blueClusterInfo = env.auroraUtil.getClusterByArn(bgDeployment.source());
    if (blueClusterInfo != null) {
      try {
        env.auroraUtil.waitUntilClusterHasRightState(blueClusterInfo.dbClusterIdentifier());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }

    DBCluster greenClusterInfo = env.auroraUtil.getClusterByArn(bgDeployment.target());
    if (greenClusterInfo != null) {
      try {
        env.auroraUtil.waitUntilClusterHasRightState(greenClusterInfo.dbClusterIdentifier());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }
  }

  private static void waitForBlueGreenInstancesHaveRightState(TestEnvironmentConfig env, BlueGreenDeployment bgDeployment) {

    DBInstance blueInstanceInfo = env.auroraUtil.getRdsInstanceInfoByArn(bgDeployment.source());
    if (blueInstanceInfo != null) {
      try {
        env.auroraUtil.waitUntilInstanceHasRightState(
            blueInstanceInfo.dbInstanceIdentifier(), "available");
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }

    DBInstance greenInstanceInfo = env.auroraUtil.getRdsInstanceInfoByArn(bgDeployment.target());
    if (greenInstanceInfo != null) {
      try {
        env.auroraUtil.waitUntilInstanceHasRightState(
            greenInstanceInfo.dbInstanceIdentifier(), "available");
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ex);
      }
    }
  }

  private static void createCustomClusterParameterGroup(TestEnvironmentConfig env) {
    String groupName = String.format("test-cpg-%s", env.info.getRandomBase());
    String engine = getDbEngine(env.info.getRequest());
    String engineVersion = getDbEngineVersion(env);
    env.auroraUtil.createCustomClusterParameterGroup(
        groupName, engine, engineVersion, env.info.getRequest().getDatabaseEngine());
    env.info.getDatabaseInfo().setClusterParameterGroupName(groupName);
  }

  private static void createDatabaseContainers(TestEnvironmentConfig env) {
    ContainerHelper containerHelper = new ContainerHelper();

    switch (env.info.getRequest().getDatabaseInstances()) {
      case SINGLE_INSTANCE:
        env.numOfInstances = 1;
        break;
      case MULTI_INSTANCE:
        env.numOfInstances = env.info.getRequest().getNumOfInstances();
        if (env.numOfInstances < 1 || env.numOfInstances > 15) {
          LOGGER.warning(
              env.numOfInstances + " instances were requested but the requested number must be "
                  + "between 1 and 15. 5 instances will be used as a default.");
          env.numOfInstances = 5;
        }
        break;
      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseInstances().toString());
    }

    switch (env.info.getRequest().getDatabaseEngine()) {
      case MYSQL:
        for (int i = 1; i <= env.numOfInstances; i++) {
          env.databaseContainers.add(
              containerHelper.createMysqlContainer(
                  env.network,
                  DATABASE_CONTAINER_NAME_PREFIX + i,
                  env.info.getDatabaseInfo().getDefaultDbName(),
                  env.info.getDatabaseInfo().getUsername(),
                  env.info.getDatabaseInfo().getPassword()));
          env.databaseContainers.get(0).start();

          env.info
              .getDatabaseInfo()
              .getInstances()
              .add(
                  new TestInstanceInfo(
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      3306));
        }
        break;

      case PG:
        for (int i = 1; i <= env.numOfInstances; i++) {
          env.databaseContainers.add(
              containerHelper.createPostgresContainer(
                  env.network,
                  DATABASE_CONTAINER_NAME_PREFIX + i,
                  env.info.getDatabaseInfo().getDefaultDbName(),
                  env.info.getDatabaseInfo().getUsername(),
                  env.info.getDatabaseInfo().getPassword()));
          env.databaseContainers.get(0).start();

          env.info
              .getDatabaseInfo()
              .getInstances()
              .add(
                  new TestInstanceInfo(
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      DATABASE_CONTAINER_NAME_PREFIX + i,
                      5432));
        }
        break;

      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseEngine().toString());
    }
  }

  private static void initEnv(TestEnvironmentConfig env) {
    env.rdsDbRegion = System.getenv("RDS_DB_REGION");
    env.info.setRegion(
        !StringUtils.isNullOrEmpty(env.rdsDbRegion)
            ? env.rdsDbRegion
            : "us-east-2");

    env.reuseDb = Boolean.parseBoolean(System.getenv("REUSE_RDS_DB"));
    env.rdsDbName = System.getenv("RDS_DB_NAME"); // "cluster-mysql", "instance-name", "cluster-multi-az-name"
    env.rdsDbDomain = System.getenv("RDS_DB_DOMAIN"); // "XYZ.us-west-2.rds.amazonaws.com"
    env.rdsEndpoint = System.getenv("RDS_ENDPOINT"); // "https://rds-int.amazon.com"

    env.auroraMySqlDbEngineVersion = System.getenv("MYSQL_VERSION");
    env.auroraPgDbEngineVersion = System.getenv("PG_VERSION");
    env.rdsMySqlDbEngineVersion = System.getenv("MYSQL_VERSION");
    env.rdsPgDbEngineVersion = System.getenv("PG_VERSION");

    env.info.setRdsEndpoint(env.rdsEndpoint);
    env.info.setRdsDbName(env.rdsDbName);

    env.auroraUtil =
        new AuroraTestUtility(
            env.info.getRegion(),
            env.rdsEndpoint,
            env.awsAccessKeyId,
            env.awsSecretAccessKey,
            env.awsSessionToken);
  }

  private static void createDbCluster(TestEnvironmentConfig env) {
    switch (env.info.getRequest().getDatabaseInstances()) {
      case SINGLE_INSTANCE:
        initAwsCredentials(env);
        env.numOfInstances = 1;
        createDbCluster(env, 1);
        break;
      case MULTI_INSTANCE:
        initAwsCredentials(env);

        env.numOfInstances = env.info.getRequest().getNumOfInstances();
        if (env.info.getRequest().getDatabaseEngineDeployment() == DatabaseEngineDeployment.AURORA ||
                env.info.getRequest().getDatabaseEngineDeployment() == DatabaseEngineDeployment.AURORA_LIMITLESS) {
          if (env.numOfInstances < 1 || env.numOfInstances > 15) {
            LOGGER.warning(
                env.numOfInstances + " instances were requested but the requested number must be "
                    + "between 1 and 15. 5 instances will be used as a default.");
            env.numOfInstances = 5;
          }
        }
        if (env.info.getRequest().getDatabaseEngineDeployment() == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER) {
          if (env.numOfInstances != 3) {
            LOGGER.warning(
                env.numOfInstances + " instances were requested but the requested number must be 3. "
                    + "3 instances will be used as a default.");
            env.numOfInstances = 3;
          }
        }

        createDbCluster(env, env.numOfInstances);
        break;
      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseEngine().toString());
    }
  }

  private static void createDbCluster(TestEnvironmentConfig env, int numOfInstances) {
    if (env.reuseDb) {
      if (StringUtils.isNullOrEmpty(env.rdsDbDomain)) {
        throw new RuntimeException("Environment variable RDS_DB_DOMAIN is required.");
      }
      if (StringUtils.isNullOrEmpty(env.rdsDbName)) {
        throw new RuntimeException("Environment variable RDS_DB_NAME is required.");
      }

      if (!env.auroraUtil.doesClusterExist(env.rdsDbName)) {
        throw new RuntimeException(
            "It's requested to reuse existing DB cluster but it doesn't exist: "
                + env.rdsDbName
                + ".cluster-"
                + env.rdsDbDomain);
      }

      DBCluster clusterInfo = env.auroraUtil.getClusterInfo(env.rdsDbName);

      DatabaseEngine existingClusterDatabaseEngine = env.auroraUtil.getClusterEngine(clusterInfo);
      if (existingClusterDatabaseEngine != env.info.getRequest().getDatabaseEngine()) {
        throw new RuntimeException(
            "Existing cluster is "
                + existingClusterDatabaseEngine
                + " cluster. "
                + env.info.getRequest().getDatabaseEngine()
                + " is expected.");
      }

      env.info.setDatabaseEngine(clusterInfo.engine());
      env.info.setDatabaseEngineVersion(clusterInfo.engineVersion());
    } else {
      if (StringUtils.isNullOrEmpty(env.rdsDbName)) {
        int remainingTries = 5;
        boolean clusterExists = false;
        while (remainingTries-- > 0) {
          env.rdsDbName = getRandomName(env.info.getRequest());
          if (env.auroraUtil.doesClusterExist(env.rdsDbName)) {
            clusterExists = true;
            env.info.setRandomBase(null);
            initRandomBase(env);
            LOGGER.finest("Cluster " + env.rdsDbName + " already exists. Pick up another name.");
          } else {
            clusterExists = false;
            LOGGER.finer("Cluster to create: " + env.rdsDbName);
            break;
          }
        }
        if (clusterExists) {
          throw new RuntimeException("Can't pick up a cluster name.");
        }
      }

      try {
        String engine = getDbEngine(env.info.getRequest());
        String engineVersion = getDbEngineVersion(env);
        if (StringUtils.isNullOrEmpty(engineVersion)) {
          throw new RuntimeException("Failed to get engine version.");
        }
        String instanceClass = AuroraTestUtility.getDbInstanceClass(env.info.getRequest());

        LOGGER.finer("Using " + engine + " " + engineVersion);

        env.auroraUtil.createCluster(
            env.info.getDatabaseInfo().getUsername(),
            env.info.getDatabaseInfo().getPassword(),
            env.info.getDatabaseInfo().getDefaultDbName(),
            env.rdsDbName,
            env.info.getRequest().getDatabaseEngineDeployment(),
            env.info.getRegion(),
            engine,
            instanceClass,
            engineVersion,
            env.info.getDatabaseInfo().getClusterParameterGroupName(),
            numOfInstances);

        if (env.info.getRequest().getDatabaseEngineDeployment() == DatabaseEngineDeployment.AURORA_LIMITLESS) {
                env.rdsDbDomain = env.auroraUtil.getAuroraLimitlessClusterDomainSuffix();
        } else {
            List<DBInstance> dbInstances = env.auroraUtil.getDBInstances(env.rdsDbName);
            if (dbInstances.isEmpty()) {
                throw new RuntimeException("Failed to get instance information for cluster " + env.rdsDbName);
            }

            final String instanceEndpoint = dbInstances.get(0).endpoint().address();
            env.rdsDbDomain = instanceEndpoint.substring(instanceEndpoint.indexOf(".") + 1);
        }
        env.info.setDatabaseEngine(engine);
        env.info.setDatabaseEngineVersion(engineVersion);
        LOGGER.finer(
            "Created a new cluster " + env.rdsDbName + ".cluster-" + env.rdsDbDomain);
      } catch (Exception e) {

        LOGGER.finer("Error creating a cluster " + env.rdsDbName + ". " + e.getMessage());

        // remove cluster and instances
        LOGGER.finer("Deleting cluster " + env.rdsDbName);
        env.auroraUtil.deleteCluster(env.rdsDbName, env.info.getRequest().getDatabaseEngineDeployment(), false);
        LOGGER.finer("Deleted cluster " + env.rdsDbName);

        throw new RuntimeException(e);
      }
    }

    env.info.setRdsDbName(env.rdsDbName);

    int port = getPort(env.info.getRequest());

    String clusterEndpoint = env.rdsDbName + ".cluster-" + env.rdsDbDomain;
    env.info
        .getDatabaseInfo()
        .setClusterEndpoint(clusterEndpoint, port);
    
    String clusterReadOnlyEndpoint = env.rdsDbName + ".cluster-ro-" + env.rdsDbDomain;
    env.info
        .getDatabaseInfo()
        .setClusterReadOnlyEndpoint(clusterReadOnlyEndpoint, port);
    env.info.getDatabaseInfo().setInstanceEndpointSuffix(env.rdsDbDomain, port);

    List<TestInstanceInfo> instances = env.auroraUtil.getTestInstancesInfo(env.rdsDbName);
    env.info.getDatabaseInfo().getInstances().clear();
    env.info.getDatabaseInfo().getInstances().addAll(instances);

    // Make sure the cluster is available and accessible.
    try {
      env.auroraUtil.waitUntilClusterHasRightState(env.rdsDbName);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }

    final DatabaseEngineDeployment deployment = env.info.getRequest().getDatabaseEngineDeployment();
    final DatabaseEngine engine = env.info.getRequest().getDatabaseEngine();
    final TestDatabaseInfo info = env.info.getDatabaseInfo();
    if ((DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER.equals(deployment)
        || DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE.equals(deployment)) && DatabaseEngine.PG.equals(engine)) {
      final String url =
          String.format(
              "%s%s:%d/%s",
              DriverHelper.getDriverProtocol(engine),
              info.getClusterEndpoint(),
              info.getClusterEndpointPort(),
              info.getDefaultDbName());

      env.auroraUtil.createRdsExtension(
          engine,
          url,
          info.getUsername(),
          info.getPassword());
    }
  }

  private static void createMultiAzInstance(TestEnvironmentConfig env) {
    env.auroraUtil =
        new AuroraTestUtility(
            env.info.getRegion(),
            env.rdsEndpoint,
            env.awsAccessKeyId,
            env.awsSecretAccessKey,
            env.awsSessionToken);

    ArrayList<TestInstanceInfo> instances = new ArrayList<>();

    if (env.reuseDb) {
      if (StringUtils.isNullOrEmpty(env.rdsDbDomain)) {
        throw new RuntimeException("Environment variable RDS_DB_DOMAIN is required.");
      }
      if (StringUtils.isNullOrEmpty(env.rdsDbName)) {
        throw new RuntimeException("Environment variable RDS_DB_NAME is required.");
      }

      if (!env.auroraUtil.doesInstanceExist(env.rdsDbName)) {
        throw new RuntimeException(
            "It's requested to reuse existing RDS instance but it doesn't exist: "
                + env.rdsDbName
                + "."
                + env.rdsDbDomain);
      }
      LOGGER.finer(
          "Reuse existing RDS Instance " + env.rdsDbName + "." + env.rdsDbDomain);

      DBInstance instanceInfo = env.auroraUtil.getRdsInstanceInfo(env.rdsDbName);

      DatabaseEngine existingRdsInstanceDatabaseEngine = env.auroraUtil.getRdsInstanceEngine(instanceInfo);
      if (existingRdsInstanceDatabaseEngine != env.info.getRequest().getDatabaseEngine()) {
        throw new RuntimeException(
            "Existing RDS Instance is "
                + existingRdsInstanceDatabaseEngine
                + " instance. "
                + env.info.getRequest().getDatabaseEngine()
                + " is expected.");
      }

      env.info.setDatabaseEngine(instanceInfo.engine());
      env.info.setDatabaseEngineVersion(instanceInfo.engineVersion());
      instances.add(new TestInstanceInfo(
          instanceInfo.dbInstanceIdentifier(),
          instanceInfo.endpoint().address(),
          instanceInfo.endpoint().port()));

    } else {
      if (StringUtils.isNullOrEmpty(env.rdsDbName)) {
        env.rdsDbName = getRandomName(env.info.getRequest());
        LOGGER.finer("RDS Instance to create: " + env.rdsDbName);
      }

      try {
        String engine = getDbEngine(env.info.getRequest());
        String engineVersion = getDbEngineVersion(env);
        if (StringUtils.isNullOrEmpty(engineVersion)) {
          throw new RuntimeException("Failed to get engine version.");
        }
        String instanceClass = AuroraTestUtility.getDbInstanceClass(env.info.getRequest());

        LOGGER.finer("Using " + engine + " " + engineVersion);

        env.rdsDbDomain =
            env.auroraUtil.createMultiAzInstance(
                env.info.getDatabaseInfo().getUsername(),
                env.info.getDatabaseInfo().getPassword(),
                env.info.getDatabaseInfo().getDefaultDbName(),
                env.rdsDbName,
                env.info.getRequest().getDatabaseEngineDeployment(),
                engine,
                instanceClass,
                engineVersion,
                instances);

        env.info.setDatabaseEngine(engine);
        env.info.setDatabaseEngineVersion(engineVersion);
        LOGGER.finer(
            "Created a new RDS Instance " + env.rdsDbName + "." + env.rdsDbDomain);
      } catch (Exception e) {

        LOGGER.finer("Error creating a RDS Instance " + env.rdsDbName + ". " + e);

        // remove RDS instance
        LOGGER.finer("Deleting RDS Instance " + env.rdsDbName);
        env.auroraUtil.deleteMultiAzInstance(env.rdsDbName, false);
        LOGGER.finer("Deleted RDS Instance " + env.rdsDbName);

        throw new RuntimeException(e);
      }
    }

    env.info.setRdsDbName(env.rdsDbName);

    int port = getPort(env.info.getRequest());
    env.info.getDatabaseInfo().setInstanceEndpointSuffix(env.rdsDbDomain, port);

    env.info.getDatabaseInfo().getInstances().clear();
    env.info.getDatabaseInfo().getInstances().addAll(instances);

    final DatabaseEngineDeployment deployment = env.info.getRequest().getDatabaseEngineDeployment();
    final DatabaseEngine engine = env.info.getRequest().getDatabaseEngine();
    final TestDatabaseInfo info = env.info.getDatabaseInfo();
    String url;
    switch (deployment) {
      case RDS_MULTI_AZ_INSTANCE:
        url =
            String.format(
                "%s%s:%d/%s",
                DriverHelper.getDriverProtocol(engine),
                instances.get(0).getHost(),
                port,
                info.getDefaultDbName());

        if (engine == DatabaseEngine.PG) {
          env.auroraUtil.createRdsExtension(
              engine,
              url,
              info.getUsername(),
              info.getPassword());
        }

        break;
      case RDS_MULTI_AZ_CLUSTER:
        url =
            String.format(
                "%s%s:%d/%s",
                DriverHelper.getDriverProtocol(engine),
                info.getClusterEndpoint(),
                port,
                info.getDefaultDbName());

        if (engine == DatabaseEngine.PG) {
          env.auroraUtil.createRdsExtension(
              engine,
              url,
              info.getUsername(),
              info.getPassword());
        }

        break;
      default:
        throw new UnsupportedOperationException(deployment.toString());
    }
  }

  private static void authorizeIP(TestEnvironmentConfig env) {
    try {
      env.runnerIP = env.auroraUtil.getPublicIPAddress();
      LOGGER.finest("Test runner IP: " + env.runnerIP);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    env.auroraUtil.ec2AuthorizeIP(env.runnerIP);
    LOGGER.finest(String.format("Test runner IP %s authorized. Usage count: %d",
        env.runnerIP, ipAddressUsageRefCount.get()));
  }

  private static void deAuthorizeIP(TestEnvironmentConfig env) {
    if (ipAddressUsageRefCount.decrementAndGet() == 0) {
      if (env.runnerIP == null) {
        try {
          env.runnerIP = env.auroraUtil.getPublicIPAddress();
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        }
      }
      if (!env.reuseDb) {
        env.auroraUtil.ec2DeauthorizesIP(env.runnerIP);
        LOGGER.finest(String.format("Test runner IP %s de-authorized. Usage count: %d",
            env.runnerIP, ipAddressUsageRefCount.get()));
      } else {
        LOGGER.finest("The IP address usage count hit 0, but the REUSE_RDS_DB was set to true, so IP "
            + "de-authorization was skipped.");
      }
    } else {
      LOGGER.finest("IP usage count: " + ipAddressUsageRefCount.get());
    }
  }

  private static void initRandomBase(TestEnvironmentConfig env) {
    String randomBase = env.info.getRandomBase();
    if (StringUtils.isNullOrEmpty(randomBase)) {
      env.info.setRandomBase(generateRandom());
    }
  }

  private static String getRandomName(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "test-mysql-" + System.nanoTime();
      case PG:
        return "test-pg-" + System.nanoTime();
      default:
        return String.valueOf(System.nanoTime());
    }
  }

  private static String generateRandom() {
    String alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";

    int n = alphabet.length();
    StringBuilder result = new StringBuilder();
    Random r = new Random();

    for (int i = 0; i < 10; i++) {
      result.append(alphabet.charAt(r.nextInt(n)));
    }

    return result.toString();
  }

  private static String getDbEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
      case AURORA_LIMITLESS:
        return getAuroraEngine(request);
      case RDS:
      case RDS_MULTI_AZ_INSTANCE:
      case RDS_MULTI_AZ_CLUSTER:
        return getEngine(request);
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }
  }

  private static String getDbEngineVersion(TestEnvironmentConfig env) {
    final TestEnvironmentRequest request = env.info.getRequest();
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
      case AURORA_LIMITLESS:
        return getAuroraDbEngineVersion(env);
      case RDS:
      case RDS_MULTI_AZ_INSTANCE:
      case RDS_MULTI_AZ_CLUSTER:
        return getRdsEngineVersion(env);
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }
  }

  private static String getRdsEngineVersion(TestEnvironmentConfig env) {
    String engineName;
    String systemPropertyVersion;
    TestEnvironmentRequest request = env.info.getRequest();
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        engineName = "mysql";
        systemPropertyVersion = env.rdsMySqlDbEngineVersion;
        break;
      case PG:
        engineName = "postgres";
        systemPropertyVersion = env.rdsPgDbEngineVersion;
        break;
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
    return findDbEngineVersion(env, engineName, systemPropertyVersion);
  }

  private static String getAuroraDbEngineVersion(TestEnvironmentConfig env) {
    String engineName;
    String systemPropertyVersion;
    TestEnvironmentRequest request = env.info.getRequest();
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        engineName = "aurora-mysql";
        systemPropertyVersion = env.auroraMySqlDbEngineVersion;
        break;
      case PG:
        engineName = "aurora-postgresql";
        systemPropertyVersion = env.auroraPgDbEngineVersion;
        break;
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
    return findDbEngineVersion(env, engineName, systemPropertyVersion);
  }

  private static String findDbEngineVersion(TestEnvironmentConfig env, String engineName, String systemPropertyVersion) {
    if (systemPropertyVersion == null) {
      return env.auroraUtil.getDefaultVersion(engineName);
    }
    switch (systemPropertyVersion.toLowerCase()) {
      case "default":
        return env.auroraUtil.getDefaultVersion(engineName);
      case "latest":
        return env.auroraUtil.getLatestVersion(engineName);
      default:
        return systemPropertyVersion;
    }
  }

  private static String getAuroraEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "aurora-mysql";
      case PG:
        return "aurora-postgresql";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return "mysql";
      case PG:
        return "postgres";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static String getDbInstanceClass(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
      case AURORA_LIMITLESS:
        return "db.r5.large";
      case RDS:
      case RDS_MULTI_AZ_CLUSTER:
        return "db.m5d.large";
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static int getPort(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngine()) {
      case MYSQL:
        return 3306;
      case PG:
        return 5432;
      default:
        throw new NotImplementedException(request.getDatabaseEngine().toString());
    }
  }

  private static void initDatabaseParams(TestEnvironmentConfig env) {
    final String dbName =
        !StringUtils.isNullOrEmpty(System.getenv("DB_DATABASE_NAME"))
            ? System.getenv("DB_DATABASE_NAME")
            : "test_database";
    final String dbUsername =
        !StringUtils.isNullOrEmpty(System.getenv("DB_USERNAME"))
            ? System.getenv("DB_USERNAME")
            : "test_user";
    final String dbPassword =
        !StringUtils.isNullOrEmpty(System.getenv("DB_PASSWORD"))
            ? System.getenv("DB_PASSWORD")
            : "secret_password";

    env.info.setDatabaseInfo(new TestDatabaseInfo());
    env.info.getDatabaseInfo().setUsername(dbUsername);
    env.info.getDatabaseInfo().setPassword(dbPassword);
    env.info.getDatabaseInfo().setDefaultDbName(dbName);
  }

  private static void initLimitlessDatabaseParams(TestEnvironmentConfig env) {
    final String dbName =
        !StringUtils.isNullOrEmpty(System.getenv("DB_DATABASE_NAME"))
            ? System.getenv("DB_DATABASE_NAME")
            : "postgres_limitless";
    final String dbUsername =
        !StringUtils.isNullOrEmpty(System.getenv("DB_USERNAME"))
            ? System.getenv("DB_USERNAME")
            : "test_user";
    final String dbPassword =
        !StringUtils.isNullOrEmpty(System.getenv("DB_PASSWORD"))
            ? System.getenv("DB_PASSWORD")
            : "secret_password";

    env.info.setDatabaseInfo(new TestDatabaseInfo());
    env.info.getDatabaseInfo().setUsername(dbUsername);
    env.info.getDatabaseInfo().setPassword(dbPassword);
    env.info.getDatabaseInfo().setDefaultDbName(dbName);
  }

  private static void initAwsCredentials(TestEnvironmentConfig env) {
    env.awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    env.awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    env.awsSessionToken = System.getenv("AWS_SESSION_TOKEN");

    if (StringUtils.isNullOrEmpty(env.awsAccessKeyId)) {
      throw new RuntimeException("Environment variable AWS_ACCESS_KEY_ID is required.");
    }
    if (StringUtils.isNullOrEmpty(env.awsSecretAccessKey)) {
      throw new RuntimeException("Environment variable AWS_SECRET_ACCESS_KEY is required.");
    }

    if (env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      env.info.setAwsAccessKeyId(env.awsAccessKeyId);
      env.info.setAwsSecretAccessKey(env.awsSecretAccessKey);
      if (!StringUtils.isNullOrEmpty(env.awsSessionToken)) {
        env.info.setAwsSessionToken(env.awsSessionToken);
      }
    }
  }

  private static void createProxyContainers(TestEnvironmentConfig env) {
    ContainerHelper containerHelper = new ContainerHelper();

    int port = getPort(env.info.getRequest());

    env.info.setProxyDatabaseInfo(new TestProxyDatabaseInfo());
    env.info.getProxyDatabaseInfo().setControlPort(PROXY_CONTROL_PORT);
    env.info.getProxyDatabaseInfo().setUsername(env.info.getDatabaseInfo().getUsername());
    env.info.getProxyDatabaseInfo().setPassword(env.info.getDatabaseInfo().getPassword());
    env.info.getProxyDatabaseInfo().setDefaultDbName(env.info.getDatabaseInfo().getDefaultDbName());

    env.proxyContainers = new ArrayList<>();

    int proxyPort = 0;
    for (TestInstanceInfo instance : env.info.getDatabaseInfo().getInstances()) {
      ToxiproxyContainer container =
          containerHelper.createProxyContainer(env.network, instance, PROXIED_DOMAIN_NAME_SUFFIX);

      container.start();
      env.proxyContainers.add(container);

      ToxiproxyContainer.ContainerProxy proxy =
          container.getProxy(instance.getHost(), instance.getPort());

      if (proxyPort != 0 && proxyPort != proxy.getOriginalProxyPort()) {
        throw new RuntimeException("DB cluster proxies should be on the same port.");
      }
      proxyPort = proxy.getOriginalProxyPort();
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getClusterEndpoint())) {
      env.proxyContainers.add(
          containerHelper.createAndStartProxyContainer(
              env.network,
              "proxy-cluster",
              env.info.getDatabaseInfo().getClusterEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              env.info.getDatabaseInfo().getClusterEndpoint(),
              port,
              proxyPort));

      env.info
          .getProxyDatabaseInfo()
          .setClusterEndpoint(
              env.info.getDatabaseInfo().getClusterEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getClusterReadOnlyEndpoint())) {
      env.proxyContainers.add(
          containerHelper.createAndStartProxyContainer(
              env.network,
              "proxy-ro-cluster",
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint(),
              port,
              proxyPort));

      env.info
          .getProxyDatabaseInfo()
          .setClusterReadOnlyEndpoint(
              env.info.getDatabaseInfo().getClusterReadOnlyEndpoint() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    if (!StringUtils.isNullOrEmpty(env.info.getDatabaseInfo().getInstanceEndpointSuffix())) {
      env.info
          .getProxyDatabaseInfo()
          .setInstanceEndpointSuffix(
              env.info.getDatabaseInfo().getInstanceEndpointSuffix() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
    }

    for (TestInstanceInfo instanceInfo : env.info.getDatabaseInfo().getInstances()) {
      TestInstanceInfo proxyInstanceInfo =
          new TestInstanceInfo(
              instanceInfo.getInstanceId(),
              instanceInfo.getHost() + PROXIED_DOMAIN_NAME_SUFFIX,
              proxyPort);
      env.info.getProxyDatabaseInfo().getInstances().add(proxyInstanceInfo);
    }
  }

  private static void createTestContainer(TestEnvironmentConfig env) {
    final ContainerHelper containerHelper = new ContainerHelper();
    env.testContainer = containerHelper.createTestContainer(
        "aws/rds-test-container",
        getContainerBaseImageName());
    env.testContainer
        .withNetworkAliases(TEST_CONTAINER_NAME)
        .withNetwork(env.network)
        .withEnv("TEST_ENV_INFO_JSON", getEnvironmentInfoAsString(env))
        .withEnv("TEST_ENV_DESCRIPTION", env.info.getRequest().getDisplayName());

    if (env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      env.testContainer
          .withEnv("AWS_ACCESS_KEY_ID", env.awsAccessKeyId)
          .withEnv("AWS_SECRET_ACCESS_KEY", env.awsSecretAccessKey)
          .withEnv("AWS_SESSION_TOKEN", env.awsSessionToken);
    }

    env.testContainer.start();
  }

  private static void createTelemetryXRayContainer(TestEnvironmentConfig env) {
    String xrayAwsRegion =
        !StringUtils.isNullOrEmpty(System.getenv("XRAY_AWS_REGION"))
            ? System.getenv("XRAY_AWS_REGION")
            : "eu-west-1";

    LOGGER.finest("Creating XRay telemetry container");
    final ContainerHelper containerHelper = new ContainerHelper();

    env.telemetryXRayContainer = containerHelper.createTelemetryXrayContainer(
        xrayAwsRegion,
        env.network,
        TELEMETRY_XRAY_CONTAINER_NAME);

    if (!env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      throw new RuntimeException("AWS_CREDENTIALS_ENABLED is required for XRay telemetry.");
    }

    env.telemetryXRayContainer
        .withEnv("AWS_ACCESS_KEY_ID", env.awsAccessKeyId)
        .withEnv("AWS_SECRET_ACCESS_KEY", env.awsSecretAccessKey)
        .withEnv("AWS_SESSION_TOKEN", env.awsSessionToken);

    env.info.setXrayTracesTelemetryInfo(new TestTelemetryInfo(TELEMETRY_XRAY_CONTAINER_NAME, 2000));
    LOGGER.finest("Starting XRay telemetry container");
    env.telemetryXRayContainer.start();
  }

  private static void createTelemetryOtlpContainer(TestEnvironmentConfig env) {

    LOGGER.finest("Creating OTLP telemetry container");
    final ContainerHelper containerHelper = new ContainerHelper();

    env.telemetryOtlpContainer = containerHelper.createTelemetryOtlpContainer(
        env.network,
        TELEMETRY_OTLP_CONTAINER_NAME);

    if (!env.info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)) {
      throw new RuntimeException("AWS_CREDENTIALS_ENABLED is required for OTLP telemetry.");
    }

    String otlpRegion = !StringUtils.isNullOrEmpty(System.getenv("OTLP_AWS_REGION"))
        ? System.getenv("OTLP_AWS_REGION")
        : "eu-west-1";

    env.telemetryOtlpContainer
        .withEnv("AWS_ACCESS_KEY_ID", env.awsAccessKeyId)
        .withEnv("AWS_SECRET_ACCESS_KEY", env.awsSecretAccessKey)
        .withEnv("AWS_SESSION_TOKEN", env.awsSessionToken)
        .withEnv("AWS_REGION", otlpRegion);

    env.info.setOtelTracesTelemetryInfo(new TestTelemetryInfo(TELEMETRY_OTLP_CONTAINER_NAME, 4317));
    env.info.setMetricsTelemetryInfo(new TestTelemetryInfo(TELEMETRY_OTLP_CONTAINER_NAME, 4317));

    LOGGER.finest("Starting OTLP telemetry container");
    env.telemetryOtlpContainer.start();
  }

  private static String getContainerBaseImageName() {
    return "golang:1.24";
  }

  private static void configureIamAccess(TestEnvironmentConfig env) {
    if (!env.info.getRequest().getFeatures().contains(TestEnvironmentFeatures.IAM)) {
      return;
    }

    final DatabaseEngineDeployment deployment = env.info.getRequest().getDatabaseEngineDeployment();

    env.info.setIamUsername(
        !StringUtils.isNullOrEmpty(System.getenv("IAM_USER"))
            ? System.getenv("IAM_USER")
            : "jane_doe");

    if (!env.reuseDb) {
      try {
        Class.forName(DriverHelper.getDriverClassname(env.info.getRequest().getDatabaseEngine()));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(
            "Driver not found: "
                + DriverHelper.getDriverClassname(env.info.getRequest().getDatabaseEngine()),
            e);
      }

      String url;
      switch (deployment) {
        case AURORA:
        case AURORA_LIMITLESS:
        case RDS_MULTI_AZ_CLUSTER:
          url = String.format(
                  "%s%s:%d/%s",
                  DriverHelper.getDriverProtocol(env.info.getRequest().getDatabaseEngine()),
                  env.info.getDatabaseInfo().getClusterEndpoint(),
                  env.info.getDatabaseInfo().getClusterEndpointPort(),
                  env.info.getDatabaseInfo().getDefaultDbName());
          break;
        case RDS_MULTI_AZ_INSTANCE:
          url = String.format(
                  "%s%s:%d/%s",
                  DriverHelper.getDriverProtocol(env.info.getRequest().getDatabaseEngine()),
                  env.info.getDatabaseInfo().getInstances().get(0).getHost(),
                  env.info.getDatabaseInfo().getInstances().get(0).getPort(),
                  env.info.getDatabaseInfo().getDefaultDbName());
          break;
        default:
          throw new UnsupportedOperationException(deployment.toString());
      }

        try {
        final boolean useRdsTools = env.info.getRequest().getFeatures()
            .contains(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)
            && env.info.getRequest().getDatabaseEngine() == DatabaseEngine.PG
            && env.info.getRequest().getDatabaseEngineDeployment() == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE;
        env.auroraUtil.addAuroraAwsIamUser(
            env.info.getRequest().getDatabaseEngine(),
            env.info.getRequest().getDatabaseEngineDeployment(),
            url,
            env.info.getDatabaseInfo().getUsername(),
            env.info.getDatabaseInfo().getPassword(),
            env.info.getIamUsername(),
            env.info.getDatabaseInfo().getDefaultDbName(),
            useRdsTools);

      } catch (SQLException e) {
        throw new RuntimeException("Error configuring IAM access.", e);
      }
    }
  }

  private static String getEnvironmentInfoAsString(TestEnvironmentConfig env) {
    try {
      final ObjectMapper mapper = new ObjectMapper();
      return mapper.writeValueAsString(env.info);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Error serializing environment details.", e);
    }
  }

  public void runTests() throws IOException, InterruptedException {
    final ContainerHelper containerHelper = new ContainerHelper();
    boolean isPerformanceTest = info.getRequest().getFeatures().contains(TestEnvironmentFeatures.PERFORMANCE);
    containerHelper.runTest(this.testContainer, isPerformanceTest);
  }

  public void debugTests() throws IOException, InterruptedException {
    final ContainerHelper containerHelper = new ContainerHelper();
    containerHelper.debugTest(this.testContainer);
  }

  @Override
  public void close() throws Exception {
      for (GenericContainer<?> container : this.databaseContainers) {
        try {
          container.stop();
        } catch (Exception ex) {
          // ignore
        }
      }
      this.databaseContainers.clear();

    if (this.telemetryXRayContainer != null) {
      this.telemetryXRayContainer.stop();
      this.telemetryXRayContainer = null;
    }

    if (this.telemetryOtlpContainer != null) {
      this.telemetryOtlpContainer.stop();
      this.telemetryOtlpContainer = null;
    }

    if (this.testContainer != null) {
      this.testContainer.stop();
      this.testContainer = null;
    }

    if (this.proxyContainers != null) {
      for (ToxiproxyContainer proxyContainer : this.proxyContainers) {
        proxyContainer.stop();
      }
      this.proxyContainers = null;
    }

    switch (this.info.getRequest().getDatabaseEngineDeployment()) {
      case AURORA:
      case RDS_MULTI_AZ_CLUSTER:
      case AURORA_LIMITLESS:
        if (this.info.getRequest().getFeatures().contains(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)
            && !StringUtils.isNullOrEmpty(this.info.getDatabaseInfo().getBlueGreenDeploymentId())) {
          deleteBlueGreenDeployment();
          deleteDbCluster(true);
          deleteCustomClusterParameterGroup(this.info.getDatabaseInfo().getClusterParameterGroupName());
        } else {
          deleteDbCluster(false);
        }
        deAuthorizeIP(this);
        break;
      case RDS:
        throw new NotImplementedException(this.info.getRequest().getDatabaseEngineDeployment().toString());
      default:
        // do nothing
    }
  }

  private void deleteDbCluster(boolean waitForCompletion) {
    if (!this.reuseDb) {
      LOGGER.finest("Deleting cluster " + this.rdsDbName + ".cluster-" + this.rdsDbDomain);
      auroraUtil.deleteCluster(
          this.rdsDbName, this.info.getRequest().getDatabaseEngineDeployment(), waitForCompletion);
      LOGGER.finest("Deleted cluster " + this.rdsDbName + ".cluster-" + this.rdsDbDomain);
    }
  }

  private void deleteBlueGreenDeployment() throws InterruptedException {

    BlueGreenDeployment blueGreenDeployment;

    switch (this.info.getRequest().getDatabaseEngineDeployment()) {
      case AURORA:
        if (this.reuseDb) {
          break;
        }

        blueGreenDeployment = auroraUtil.getBlueGreenDeployment(this.info.getDatabaseInfo().getBlueGreenDeploymentId());

        if (blueGreenDeployment == null) {
          return;
        }

        auroraUtil.deleteBlueGreenDeployment(this.info.getDatabaseInfo().getBlueGreenDeploymentId(), true);

        // Remove extra DB cluster

        // For BGD in AVAILABLE status: source = blue, target = green
        // For BGD in SWITCHOVER_COMPLETED: source = old1, target = blue
        LOGGER.finest("BG source: " + blueGreenDeployment.source());
        LOGGER.finest("BG target: " + blueGreenDeployment.target());

        if ("SWITCHOVER_COMPLETED".equals(blueGreenDeployment.status())) {
          // Delete old1 cluster
          DBCluster old1ClusterInfo = auroraUtil.getClusterByArn(blueGreenDeployment.source());
          if (old1ClusterInfo != null) {
            auroraUtil.waitUntilClusterHasRightState(old1ClusterInfo.dbClusterIdentifier(), "available");
            LOGGER.finest("Deleting Aurora cluster " + old1ClusterInfo.dbClusterIdentifier());
            auroraUtil.deleteCluster(
                old1ClusterInfo.dbClusterIdentifier(),
                this.info.getRequest().getDatabaseEngineDeployment(),
                true);
            LOGGER.finest("Deleted Aurora cluster " + old1ClusterInfo.dbClusterIdentifier());
          }
        } else {
          // Delete green cluster
          DBCluster greenClusterInfo = auroraUtil.getClusterByArn(blueGreenDeployment.target());
          if (greenClusterInfo != null) {
            auroraUtil.promoteClusterToStandalone(blueGreenDeployment.target());
            LOGGER.finest("Deleting Aurora cluster " + greenClusterInfo.dbClusterIdentifier());
            auroraUtil.deleteCluster(
                greenClusterInfo.dbClusterIdentifier(),
                this.info.getRequest().getDatabaseEngineDeployment(),
                true);
            LOGGER.finest("Deleted Aurora cluster " + greenClusterInfo.dbClusterIdentifier());
          }
        }
        break;
      case RDS_MULTI_AZ_INSTANCE:
        if (this.reuseDb) {
          break;
        }

        blueGreenDeployment = auroraUtil.getBlueGreenDeployment(this.info.getDatabaseInfo().getBlueGreenDeploymentId());

        if (blueGreenDeployment == null) {
          return;
        }

        auroraUtil.deleteBlueGreenDeployment(this.info.getDatabaseInfo().getBlueGreenDeploymentId(), true);

        // For BGD in AVAILABLE status: source = blue, target = green
        // For BGD in SWITCHOVER_COMPLETED: source = old1, target = blue
        LOGGER.finest("BG source: " + blueGreenDeployment.source());
        LOGGER.finest("BG target: " + blueGreenDeployment.target());

        if ("SWITCHOVER_COMPLETED".equals(blueGreenDeployment.status())) {
          // Delete old1 cluster
          DBInstance old1InstanceInfo = auroraUtil.getRdsInstanceInfoByArn(blueGreenDeployment.source());
          if (old1InstanceInfo != null) {
            LOGGER.finest("Deleting MultiAz Instance " + old1InstanceInfo.dbInstanceIdentifier());
            auroraUtil.deleteMultiAzInstance(old1InstanceInfo.dbInstanceIdentifier(), true);
            LOGGER.finest("Deleted MultiAz Instance " + old1InstanceInfo.dbInstanceIdentifier());
          }
        } else {
          // Delete green cluster
          DBInstance greenInstanceInfo = auroraUtil.getRdsInstanceInfoByArn(blueGreenDeployment.target());
          if (greenInstanceInfo != null) {
            auroraUtil.promoteInstanceToStandalone(blueGreenDeployment.target());
            LOGGER.finest("Deleting MultiAz Instance " + greenInstanceInfo.dbInstanceIdentifier());
            auroraUtil.deleteMultiAzInstance(greenInstanceInfo.dbInstanceIdentifier(), true);
            LOGGER.finest("Deleted MultiAz Instance " + greenInstanceInfo.dbInstanceIdentifier());
          }
        }
        break;
      default:
        throw new RuntimeException("Unsupported " + this.info.getRequest().getDatabaseEngineDeployment());
    }
  }

  private void deleteCustomClusterParameterGroup(String groupName) {
    if (this.reuseDb) {
      return;
    }
    try {
      this.auroraUtil.deleteCustomClusterParameterGroup(groupName);
    } catch (Exception ex) {
      LOGGER.finest(String.format("Error deleting cluster parameter group %s. %s", groupName, ex));
    }
  }
}
