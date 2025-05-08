
package integration.host;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

public class TestEnvironmentProvider implements TestTemplateInvocationContextProvider {

  private static final Logger LOGGER = Logger.getLogger(TestEnvironmentProvider.class.getName());

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return true;
  }

  @Override
  public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
      ExtensionContext context) {
    ArrayList<TestTemplateInvocationContext> resultContextList = new ArrayList<>();

    final String numInstancesVar = System.getenv("NUM_INSTANCES");
    final Integer numInstances = numInstancesVar == null ? null : Integer.parseInt(numInstancesVar);
    final Set<Integer> validNumInstances = new HashSet<>(Arrays.asList(1, 2, 3, 5));
    if (numInstances != null && !validNumInstances.contains(numInstances)) {
      throw new RuntimeException(
          String.format(
              "The NUM_INSTANCES environment variable was set to an invalid value: %d. Valid values are: %s.",
              numInstances, validNumInstances));
    } else if (numInstances != null) {
      System.out.println(String.format(
          "The NUM_INSTANCES environment variable was set to %d. All test configurations for different cluster sizes will be skipped.",
          numInstances));
    }

    final boolean excludeDocker = Boolean.parseBoolean(System.getProperty("exclude-docker", "false"));
    final boolean excludeAurora = Boolean.parseBoolean(System.getProperty("exclude-aurora", "false"));
    final boolean excludeMultiAZ = Boolean.parseBoolean(System.getProperty("exclude-multi-az", "false"));
    final boolean excludePerformance =
        Boolean.parseBoolean(System.getProperty("exclude-performance", "false"));
    final boolean excludeMysqlEngine =
        Boolean.parseBoolean(System.getProperty("exclude-mysql-engine", "false"));
    final boolean excludeMysqlDriver =
        Boolean.parseBoolean(System.getProperty("exclude-mysql-driver", "false"));
    final boolean excludePgEngine =
        Boolean.parseBoolean(System.getProperty("exclude-pg-engine", "false"));
    final boolean excludePgDriver =
        Boolean.parseBoolean(System.getProperty("exclude-pg-driver", "false"));
    final boolean excludeFailover =
        Boolean.parseBoolean(System.getProperty("exclude-failover", "false"));
    final boolean excludeIam = Boolean.parseBoolean(System.getProperty("exclude-iam", "false"));
    final boolean excludeSecretsManager =
        Boolean.parseBoolean(System.getProperty("exclude-secrets-manager", "false"));
    final boolean testAutoscalingOnly = Boolean.parseBoolean(System.getProperty("test-autoscaling", "false"));
    final boolean excludeTracesTelemetry = Boolean.parseBoolean(System.getProperty("test-no-traces-telemetry", "false"));
    final boolean excludeMetricsTelemetry = Boolean.parseBoolean(System.getProperty("test-no-metrics-telemetry", "false"));

    for (DatabaseEngineDeployment deployment : DatabaseEngineDeployment.values()) {
      if (deployment == DatabaseEngineDeployment.DOCKER && excludeDocker) {
        continue;
      }
      if (deployment == DatabaseEngineDeployment.AURORA && excludeAurora) {
        continue;
      }
      if (deployment == DatabaseEngineDeployment.RDS) {
        // Not in use.
        continue;
      }
      if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER && excludeMultiAZ) {
        continue;
      }
      for (DatabaseEngine engine : DatabaseEngine.values()) {
        if (engine == DatabaseEngine.PG && excludePgEngine) {
          continue;
        }
        if (engine == DatabaseEngine.MYSQL && excludeMysqlEngine) {
          continue;
        }

        for (DatabaseInstances instances : DatabaseInstances.values()) {
          if (deployment == DatabaseEngineDeployment.DOCKER
              && instances != DatabaseInstances.SINGLE_INSTANCE) {
            continue;
          }

          final List<Integer> instancesToTest = numInstances != null ? Arrays.asList(numInstances) : Arrays.asList(1, 2, 3, 5);
          for (int numOfInstances : instancesToTest) {
            if (instances == DatabaseInstances.SINGLE_INSTANCE && numOfInstances > 1) {
              continue;
            }
            if (instances == DatabaseInstances.MULTI_INSTANCE && numOfInstances == 1) {
              continue;
            }
            if (deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER && numOfInstances != 3) {
              // Multi-AZ clusters supports only 3 instances
              continue;
            }
            if (deployment == DatabaseEngineDeployment.AURORA && numOfInstances == 3) {
              // Aurora supports clusters with 3 instances but running such tests is similar
              // to running tests on 5-instance cluster.
              // Let's save some time and skip tests for this configuration
              continue;
            }

            resultContextList.add(
              getEnvironment(
                  new TestEnvironmentRequest(
                      engine,
                      instances,
                      instances == DatabaseInstances.SINGLE_INSTANCE ? 1 : numOfInstances,
                      deployment,
                      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                      TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED,
                      deployment == DatabaseEngineDeployment.DOCKER ? null : TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
                      deployment == DatabaseEngineDeployment.DOCKER || excludeFailover
                          ? null
                          : TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                      deployment == DatabaseEngineDeployment.DOCKER
                          || deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER
                          || excludeIam
                          ? null
                          : TestEnvironmentFeatures.IAM,
                      excludeSecretsManager ? null : TestEnvironmentFeatures.SECRETS_MANAGER,
                      excludePerformance ? null : TestEnvironmentFeatures.PERFORMANCE,
                      excludeMysqlDriver ? TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS : null,
                      excludePgDriver ? TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS : null,
                      testAutoscalingOnly ? TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY : null,
                      excludeTracesTelemetry ? null : TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED,
                      excludeMetricsTelemetry ? null : TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED,
                      // AWS credentials are required for XRay telemetry
                      excludeTracesTelemetry && excludeMetricsTelemetry ? null : TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED)));
          }
        }
      }
    }

    int index = 1;
    for (TestTemplateInvocationContext testTemplateInvocationContext : resultContextList) {
      LOGGER.finest(
          "Added to the test queue: " + testTemplateInvocationContext.getDisplayName(index++));
    }

    return Arrays.stream(resultContextList.toArray(new TestTemplateInvocationContext[0]));
  }

  private TestTemplateInvocationContext getEnvironment(TestEnvironmentRequest info) {
    return new TestTemplateInvocationContext() {
      @Override
      public String getDisplayName(int invocationIndex) {
        return String.format("[%d] - %s", invocationIndex, info.getDisplayName());
      }

      @Override
      public List<Extension> getAdditionalExtensions() {
        return Collections.singletonList(new GenericTypedParameterResolver<>(info));
      }
    };
  }
}
