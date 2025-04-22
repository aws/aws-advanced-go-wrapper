package integration.host;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import integration.host.util.AuroraTestUtility;
import integration.host.util.ContainerHelper;
import integration.host.util.StringUtils;
import software.amazon.awssdk.services.rds.model.DBCluster;

public class TestEnvironmentConfig implements AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(TestEnvironmentConfig.class.getName());

  private static final String DATABASE_CONTAINER_NAME_PREFIX = "database-container-";
  private static final String TEST_CONTAINER_NAME = "test-container";
  private static final String TELEMETRY_XRAY_CONTAINER_NAME = "xray-daemon";
  private static final String TELEMETRY_OTLP_CONTAINER_NAME = "otlp-daemon";
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static final boolean USE_OTLP_CONTAINER_FOR_TRACES = true;
  protected static final int PROXY_CONTROL_PORT = 8474;
  private final TestEnvironmentInfo info =
      new TestEnvironmentInfo(); // only this info is passed to test container

  // The following variables are local to host portion of test environment. They are not shared with a
  // test container.

  private int numOfInstances;
  private boolean reuseAuroraDbCluster;
  private String auroraClusterName; // "cluster-mysql"
  private String auroraClusterDomain; // "XYZ.us-west-2.rds.amazonaws.com"

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

  public static TestEnvironmentConfig build(TestEnvironmentRequest request) throws URISyntaxException, SQLException {
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

        break;
      case AURORA:
      case RDS_MULTI_AZ_CLUSTER:
        initDatabaseParams(env);
        createDbCluster(env);
        request.getFeatures().add(TestEnvironmentFeatures.RDS_MULTI_AZ_CLUSTER_SUPPORTED);

        if (request.getFeatures().contains(TestEnvironmentFeatures.IAM)) {
          if (request.getDatabaseEngineDeployment() == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER) {
            throw new RuntimeException("IAM isn't supported by " + DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER);
          }
          configureIamAccess(env);
        }

        break;
      default:
        throw new NotImplementedException(request.getDatabaseEngineDeployment().toString());
    }

    if (request.getFeatures().contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
      createProxyContainers(env);
    }

    if (!USE_OTLP_CONTAINER_FOR_TRACES
        && request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED)) {
      createTelemetryXRayContainer(env);
    }

    if ((USE_OTLP_CONTAINER_FOR_TRACES
        && request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED))
        || request.getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED)) {
      createTelemetryOtlpContainer(env);
    }

    createTestContainer(env);

    return env;
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

  private static void createDbCluster(TestEnvironmentConfig env) throws URISyntaxException, SQLException {

    switch (env.info.getRequest().getDatabaseInstances()) {
      case SINGLE_INSTANCE:
        initAwsCredentials(env);
        env.numOfInstances = 1;
        createDbCluster(env, 1);
        break;
      case MULTI_INSTANCE:
        initAwsCredentials(env);

        env.numOfInstances = env.info.getRequest().getNumOfInstances();
        if (env.numOfInstances < 1 || env.numOfInstances > 15) {
          LOGGER.warning(
              env.numOfInstances + " instances were requested but the requested number must be "
                  + "between 1 and 15. 5 instances will be used as a default.");
          env.numOfInstances = 5;
        }

        createDbCluster(env, env.numOfInstances);
        break;
      default:
        throw new NotImplementedException(env.info.getRequest().getDatabaseEngine().toString());
    }
  }

  private static void createDbCluster(TestEnvironmentConfig env, int numOfInstances) throws URISyntaxException, SQLException {

    env.info.setRegion(
        !StringUtils.isNullOrEmpty(System.getenv("RDS_DB_REGION"))
            ? System.getenv("RDS_DB_REGION")
            : "us-east-1");

    env.reuseAuroraDbCluster =
        !StringUtils.isNullOrEmpty(System.getenv("REUSE_RDS_CLUSTER"))
            && Boolean.parseBoolean(System.getenv("REUSE_RDS_CLUSTER"));
    env.auroraClusterName = System.getenv("RDS_CLUSTER_NAME"); // "cluster-mysql"
    env.auroraClusterDomain =
        System.getenv("RDS_CLUSTER_DOMAIN"); // "XYZ.us-west-2.rds.amazonaws.com"
    env.auroraMySqlDbEngineVersion =
        System.getenv("AURORA_MYSQL_DB_ENGINE_VERSION"); // "latest", "default"
    env.auroraPgDbEngineVersion =
        System.getenv("AURORA_PG_DB_ENGINE_VERSION");
    env.rdsMySqlDbEngineVersion = System.getenv("RDS_MYSQL_DB_ENGINE_VERSION"); // "latest", "default"
    env.rdsPgDbEngineVersion = System.getenv("RDS_PG_DB_ENGINE_VERSION");

    env.auroraUtil =
        new AuroraTestUtility(
            env.info.getRegion(),
            env.info.getRdsEndpoint(),
            env.awsAccessKeyId,
            env.awsSecretAccessKey,
            env.awsSessionToken);

    ArrayList<TestInstanceInfo> instances = new ArrayList<>();

    if (env.reuseAuroraDbCluster) {
      if (StringUtils.isNullOrEmpty(env.auroraClusterDomain)) {
        throw new RuntimeException("Environment variable RDS_CLUSTER_DOMAIN is required when testing against an existing Aurora DB cluster.");
      }
      if (!env.auroraUtil.doesClusterExist(env.auroraClusterName)) {
        throw new RuntimeException(
            "It's requested to reuse existing DB cluster but it doesn't exist: "
                + env.auroraClusterName
                + "."
                + env.auroraClusterDomain);
      }
      LOGGER.finer(
          "Reuse existing cluster " + env.auroraClusterName + ".cluster-" + env.auroraClusterDomain);

      DBCluster clusterInfo = env.auroraUtil.getClusterInfo(env.auroraClusterName);

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
      instances.addAll(env.auroraUtil.getClusterInstanceIds(env.auroraClusterName));

    } else {
      if (StringUtils.isNullOrEmpty(env.auroraClusterName)) {
        env.auroraClusterName = getRandomName(env.info.getRequest());
        LOGGER.finer("Cluster to create: " + env.auroraClusterName);
      }

      try {
        final TestEnvironmentRequest request = env.info.getRequest();
        String engine = getDbEngine(request);
        String engineVersion = getDbEngineVersion(env);
        if (StringUtils.isNullOrEmpty(engineVersion)) {
          throw new RuntimeException("Failed to get engine version.");
        }

        LOGGER.finer("Using " + engine + " " + engineVersion);

        String instanceClass = getDbInstanceClass(env.info.getRequest());

        env.auroraClusterDomain =
            env.auroraUtil.createCluster(
                env.info.getDatabaseInfo().getUsername(),
                env.info.getDatabaseInfo().getPassword(),
                env.info.getDatabaseInfo().getDefaultDbName(),
                env.auroraClusterName,
                env.info.getRequest().getDatabaseEngineDeployment(),
                engine,
                instanceClass,
                engineVersion,
                numOfInstances,
                instances);
        env.info.setDatabaseEngine(engine);
        env.info.setDatabaseEngineVersion(engineVersion);
        LOGGER.finer(
            "Created a new cluster " + env.auroraClusterName + ".cluster-" + env.auroraClusterDomain);
      } catch (Exception e) {

        LOGGER.finer("Error creating a cluster " + env.auroraClusterName + ". " + e.getMessage());

        // remove cluster and instances
        LOGGER.finer("Deleting cluster " + env.auroraClusterName);
        env.auroraUtil.deleteCluster(env.auroraClusterName);
        LOGGER.finer("Deleted cluster " + env.auroraClusterName);

        throw new RuntimeException(e);
      }
    }

    env.info.setAuroraClusterName(env.auroraClusterName);

    int port = getPort(env.info.getRequest());

    env.info
        .getDatabaseInfo()
        .setClusterEndpoint(env.auroraClusterName + ".cluster-" + env.auroraClusterDomain, port);
    env.info
        .getDatabaseInfo()
        .setClusterReadOnlyEndpoint(
            env.auroraClusterName + ".cluster-ro-" + env.auroraClusterDomain, port);
    env.info.getDatabaseInfo().setInstanceEndpointSuffix(env.auroraClusterDomain, port);

    env.info.getDatabaseInfo().getInstances().clear();
    env.info.getDatabaseInfo().getInstances().addAll(instances);

    try {
      env.runnerIP = env.auroraUtil.getPublicIPAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    env.auroraUtil.ec2AuthorizeIP(env.runnerIP);

    final DatabaseEngineDeployment deployment = env.info.getRequest().getDatabaseEngineDeployment();
    final DatabaseEngine engine = env.info.getRequest().getDatabaseEngine();
    final TestDatabaseInfo info = env.info.getDatabaseInfo();

    if (DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER.equals(deployment) && DatabaseEngine.PG.equals(engine)) {
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

  private static String getDbEngine(TestEnvironmentRequest request) {
    switch (request.getDatabaseEngineDeployment()) {
      case AURORA:
        return getAuroraEngine(request);
      case RDS:
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
        return getAuroraDbEngineVersion(env);
      case RDS:
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
    return findDbEngineVersion(env, engineName, systemPropertyVersion.toLowerCase());
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
    return findDbEngineVersion(env, engineName, systemPropertyVersion.toLowerCase());
  }

  private static String findDbEngineVersion(TestEnvironmentConfig env, String engineName, String systemPropertyVersion) {
    if (systemPropertyVersion == null) {
      return env.auroraUtil.getDefaultVersion(engineName);
    }
    switch (systemPropertyVersion) {
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
    final TestEnvironmentRequest request = env.info.getRequest();

    env.testContainer = containerHelper.createTestContainer(
        "aws/rds-test-container",
        getContainerBaseImageName(request));
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
            : "us-east-1";

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

    env.info.setTracesTelemetryInfo(new TestTelemetryInfo(TELEMETRY_XRAY_CONTAINER_NAME, 2000));
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
        : "us-east-1";

    env.telemetryOtlpContainer
        .withEnv("AWS_ACCESS_KEY_ID", env.awsAccessKeyId)
        .withEnv("AWS_SECRET_ACCESS_KEY", env.awsSecretAccessKey)
        .withEnv("AWS_SESSION_TOKEN", env.awsSessionToken)
        .withEnv("AWS_REGION", otlpRegion);

    env.info.setTracesTelemetryInfo(new TestTelemetryInfo(TELEMETRY_OTLP_CONTAINER_NAME, 4317));
    env.info.setMetricsTelemetryInfo(new TestTelemetryInfo(TELEMETRY_OTLP_CONTAINER_NAME, 4317));

    LOGGER.finest("Starting OTLP telemetry container");
    env.telemetryOtlpContainer.start();
  }

  private static String getContainerBaseImageName(TestEnvironmentRequest request) {
    return "golang:1.24";
  }

  private static void configureIamAccess(TestEnvironmentConfig env) {

    if (env.info.getRequest().getDatabaseEngineDeployment() != DatabaseEngineDeployment.AURORA) {
      throw new UnsupportedOperationException(
          env.info.getRequest().getDatabaseEngineDeployment().toString());
    }

    env.info.setIamUsername(
        !StringUtils.isNullOrEmpty(System.getenv("IAM_USER"))
            ? System.getenv("IAM_USER")
            : "jane_doe");
    if (!env.reuseAuroraDbCluster) {
      final String url =
          String.format(
              "%s%s:%d/%s",
              DriverHelper.getDriverProtocol(env.info.getRequest().getDatabaseEngine()),
              env.info.getDatabaseInfo().getClusterEndpoint(),
              env.info.getDatabaseInfo().getClusterEndpointPort(),
              env.info.getDatabaseInfo().getDefaultDbName());

      try {
        env.auroraUtil.addAuroraAwsIamUser(
            env.info.getRequest().getDatabaseEngine(),
            url,
            env.info.getDatabaseInfo().getUsername(),
            env.info.getDatabaseInfo().getPassword(),
            env.info.getIamUsername(),
            env.info.getDatabaseInfo().getDefaultDbName());

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

  public void runTests(String folderName) throws IOException, InterruptedException {
    final ContainerHelper containerHelper = new ContainerHelper();
    containerHelper.runTest(this.testContainer, folderName, this);
  }

  public void debugTests(String folderName) throws IOException, InterruptedException {
    final ContainerHelper containerHelper = new ContainerHelper();
    containerHelper.debugTest(this.testContainer, folderName, this);
  }

  @Override
  public void close() throws Exception {
    if (this.databaseContainers != null) {
      for (GenericContainer<?> container : this.databaseContainers) {
        try {
          container.stop();
        } catch (Exception ex) {
          // ignore
        }
      }
      this.databaseContainers.clear();
    }

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
        deleteAuroraDbCluster();
        break;
      case RDS:
        throw new NotImplementedException(this.info.getRequest().getDatabaseEngineDeployment().toString());
      default:
        // do nothing
    }
  }

  private void deleteAuroraDbCluster() {
    if (!this.reuseAuroraDbCluster && !StringUtils.isNullOrEmpty(this.runnerIP)) {
      auroraUtil.ec2DeauthorizesIP(runnerIP);
    }

    if (!this.reuseAuroraDbCluster) {
      LOGGER.finest("Deleting cluster " + this.auroraClusterName + ".cluster-" + this.auroraClusterDomain);
      auroraUtil.deleteCluster(this.auroraClusterName);
      LOGGER.finest("Deleted cluster " + this.auroraClusterName + ".cluster-" + this.auroraClusterDomain);
    }
  }
}
