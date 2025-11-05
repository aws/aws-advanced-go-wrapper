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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.DockerException;
import integration.host.TestInstanceInfo;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testcontainers.utility.TestEnvironment;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ContainerHelper {

  private static final String MYSQL_CONTAINER_IMAGE_NAME = "mysql:latest";
  private static final String POSTGRES_CONTAINER_IMAGE_NAME = "postgres:latest";
  private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0");

  private static final String XRAY_TELEMETRY_IMAGE_NAME = "amazon/aws-xray-daemon";
  private static final String OTLP_TELEMETRY_IMAGE_NAME = "amazon/aws-otel-collector";

  private static final String INTEGRATION_TEST_TIMEOUT = "90m";
  private static final String PERFORMANCE_TEST_TIMEOUT = "12h";

  private static final String PERFORMANCE_TEST_TAG = "-tags=performance";
  private static final String PERFORMANCE_TEST_FILTER = "^TestPerformance";

  public Long runCmd(GenericContainer<?> container, String... cmd)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Long exitCode = execInContainer(container, consumer, cmd);
    System.out.println("==== Container console feed ==== <<<<");
    return exitCode;
  }

  public Long runCmdInDirectory(GenericContainer<?> container, String workingDirectory, String... cmd)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Long exitCode = execInContainer(container, workingDirectory, consumer, cmd);
    System.out.println("==== Container console feed ==== <<<<");
    return exitCode;
  }

  public void runTest(GenericContainer<?> container, boolean isPerformanceTest)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer(true);

    final String filter = System.getenv("FILTER");
    String timeout = System.getenv("TIMEOUT");
    if (timeout == null ) {
        if (isPerformanceTest) {
            timeout = PERFORMANCE_TEST_TIMEOUT;
        } else {
            timeout = INTEGRATION_TEST_TIMEOUT;
        }
    }


    Long exitCode;
      exitCode = execInContainer(container, "/app/.test/", consumer, "go", "env", "-w", "GOPROXY=direct");
    if (isPerformanceTest) {
      exitCode = execInContainer(container, "/app/.test/", consumer, "go", "test", "-timeout", timeout, PERFORMANCE_TEST_TAG, "-run", PERFORMANCE_TEST_FILTER,  "-v", "./test_framework/container/tests...");
    } else if (filter != null) {
      exitCode = execInContainer(container, "/app/.test/", consumer, "go", "test", "-timeout", timeout, "-v", "./test_framework/container/tests...", "-run", filter);
    } else {
      // Run all tests located in aws-advanced-go-wrapper/.test/test_framework/container.
      exitCode = execInContainer(container, "/app/.test/", consumer, "go", "test", "-timeout", timeout, "-v", "./test_framework/container/tests...");
    }

    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  public void debugTest(GenericContainer<?> container)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer(true);

    // Install Delve debugger.
    execInContainer(container, consumer, "go", "install", "github.com/go-delve/delve/cmd/dlv@latest");

    final String filter = System.getenv("FILTER");

    Long exitCode;
    if (filter != null) {
      exitCode = execInContainer(container, "/app/.test/", consumer, "dlv", "test", "--headless", "--listen=:5005", "--api-version=2",
              "./test_framework/container/tests...",  "--", "-test.run", filter);

    } else {
      // Debug all tests located in aws-advanced-go-wrapper/.test/test_framework/container.
      exitCode = exitCode = execInContainer(container, "/app/.test/", consumer, "dlv", "test", "--headless", "--listen=:5005",
              "--api-version=2", "./test_framework/container/tests...");
    }

    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  public GenericContainer<?> createTestContainer(String dockerImageName, String testContainerImageName) {
    return createTestContainer(
        dockerImageName,
        testContainerImageName,
        builder -> builder // Return directly, do not append extra run commands to the docker builder.
    );
  }

  public GenericContainer<?> createTestContainer(
      String dockerImageName,
      String testContainerImageName,
      Function<DockerfileBuilder, DockerfileBuilder> appendExtraCommandsToBuilder) {
    class FixedExposedPortContainer<T extends GenericContainer<T>> extends GenericContainer<T> {

      public FixedExposedPortContainer(ImageFromDockerfile withDockerfileFromBuilder) {
        super(withDockerfileFromBuilder);
      }

      public T withFixedExposedPort(int hostPort, int containerPort) {
        super.addFixedExposedPort(hostPort, containerPort, InternetProtocol.TCP);

        return self();
      }
    }

    FixedExposedPortContainer container = (FixedExposedPortContainer) new FixedExposedPortContainer<>(
        new ImageFromDockerfile(dockerImageName, true)
            .withDockerfileFromBuilder(
                builder -> appendExtraCommandsToBuilder.apply(
                    builder
                        .from(testContainerImageName)
                        .run("mkdir", "app")
                        .workDir("/app")
                        .env("PATH", "${PATH}:/root/.local/bin")
                        .entryPoint("/bin/sh -c \"while true; do sleep 30; done;\"")
                        .expose(5005))
                    .build()))
        .withFixedExposedPort(5005, 5005);

    return container
        .withEnv("LOG_LEVEL", "silly")
        .withEnv("UV_THREADPOOL_SIZE", "10")
        .withFileSystemBind("../../../xray", "/app/xray", BindMode.READ_ONLY)
        .withFileSystemBind("../../../otlp", "/app/otlp", BindMode.READ_ONLY)
        .withFileSystemBind("../../../okta", "/app/okta", BindMode.READ_ONLY)
        .withFileSystemBind("../../../pgx-driver", "/app/pgx-driver", BindMode.READ_ONLY)
        .withFileSystemBind("../../../mysql-driver", "/app/mysql-driver", BindMode.READ_ONLY)
        .withFileSystemBind("../../../iam", "/app/iam", BindMode.READ_ONLY)
        .withFileSystemBind("../../../federated-auth", "/app/federated-auth", BindMode.READ_ONLY)
        .withFileSystemBind("../../../aws-secrets-manager", "/app/aws-secrets-manager", BindMode.READ_ONLY)
        .withFileSystemBind("../../../auth-helpers", "/app/auth-helpers", BindMode.READ_ONLY)
        .withFileSystemBind("../../../awssql", "/app/awssql", BindMode.READ_ONLY)
        .withFileSystemBind("../../../.test/go.mod", "/app/.test/go.mod", BindMode.READ_ONLY)
        .withFileSystemBind("../../../.test/go.sum", "/app/.test/go.sum", BindMode.READ_ONLY)
        .withFileSystemBind("../../../.test/test_framework/host/src/test/resources/global-bundle.pem", "/app/global-bundle.pem", BindMode.READ_ONLY)
        .withFileSystemBind("../../../.test/test_framework/container", "/app/.test/test_framework/container", BindMode.READ_WRITE)
        .withPrivilegedMode(true); // Required to control Linux core settings like TcpKeepAlive
  }

  protected Long execInContainer(
      GenericContainer<?> container, String workingDirectory, Consumer<OutputFrame> consumer, String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container.getContainerInfo(), consumer, workingDirectory, command);
  }

  protected Long execInContainer(
      GenericContainer<?> container,
      Consumer<OutputFrame> consumer,
      String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container.getContainerInfo(), consumer, null, command);
  }

  protected Long execInContainer(
      InspectContainerResponse containerInfo,
      Consumer<OutputFrame> consumer,
      String workingDir,
      String... command)
      throws UnsupportedOperationException, IOException, InterruptedException {
    if (!TestEnvironment.dockerExecutionDriverSupportsExec()) {
      // at time of writing, this is the expected result in CircleCI.
      throw new UnsupportedOperationException(
          "Your docker daemon is running the \"lxc\" driver, which doesn't support \"docker exec\".");
    }

    if (!isRunning(containerInfo)) {
      throw new IllegalStateException(
          "execInContainer can only be used while the Container is running");
    }

    final String containerId = containerInfo.getId();
    final DockerClient dockerClient = DockerClientFactory.instance().client();

    final ExecCreateCmd cmd = dockerClient
        .execCreateCmd(containerId)
        .withAttachStdout(true)
        .withAttachStderr(true)
        .withEnv(Arrays.asList("JEST_HTML_REPORTER_OUTPUT_PATH", "./.test/tests/integration/container/reports/hello.html"))
        .withCmd(command);

    if (!StringUtils.isNullOrEmpty(workingDir)) {
      cmd.withWorkingDir(workingDir);
    }

    final ExecCreateCmdResponse execCreateCmdResponse = cmd.exec();
    try (final FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
      callback.addConsumer(OutputFrame.OutputType.STDOUT, consumer);
      callback.addConsumer(OutputFrame.OutputType.STDERR, consumer);
      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
    }

    Long exitCode = dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec().getExitCodeLong();
    if (exitCode != 0) {
      // Wait 5 minutes in case there are any dangling promises waiting to complete.
      Thread.sleep(5 * 60 * 1000);
      exitCode = dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec().getExitCodeLong();
    }

    return exitCode;
  }

  protected boolean isRunning(InspectContainerResponse containerInfo) {
    try {
      return containerInfo != null
          && containerInfo.getState() != null
          && containerInfo.getState().getRunning();
    } catch (DockerException e) {
      return false;
    }
  }

  public MySQLContainer<?> createMysqlContainer(
      Network network, String networkAlias, String testDbName, String username, String password) {

    return new MySQLContainer<>(MYSQL_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(testDbName)
        .withPassword(password)
        .withUsername(username)
        .withCopyFileToContainer(
            MountableFile.forHostPath("./src/test/config/standard-mysql-grant-root.sql"),
            "/docker-entrypoint-initdb.d/standard-mysql-grant-root.sql")
        .withCommand(
            "--local_infile=1",
            "--max_allowed_packet=40M",
            "--max-connections=2048",
            "--secure-file-priv=/var/lib/mysql",
            "--log_bin_trust_function_creators=1",
            "--character-set-server=utf8mb4",
            "--collation-server=utf8mb4_0900_as_cs",
            "--skip-character-set-client-handshake",
            "--log-error-verbosity=4");
  }

  public PostgreSQLContainer<?> createPostgresContainer(
      Network network, String networkAlias, String testDbName, String username, String password) {

    return new PostgreSQLContainer<>(POSTGRES_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(testDbName)
        .withUsername(username)
        .withPassword(password);
  }

  public ToxiproxyContainer createAndStartProxyContainer(
      final Network network,
      String networkAlias,
      String networkUrl,
      String hostname,
      int port,
      int expectedProxyPort) {
    final ToxiproxyContainer container = new ToxiproxyContainer(TOXIPROXY_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(networkAlias, networkUrl);
    container.start();
    ToxiproxyContainer.ContainerProxy proxy = container.getProxy(hostname, port);
    assertEquals(
        expectedProxyPort,
        proxy.getOriginalProxyPort(),
        "Proxy port for " + hostname + " should be " + expectedProxyPort);
    return container;
  }

  public ToxiproxyContainer createProxyContainer(
      final Network network, TestInstanceInfo instance, String proxyDomainNameSuffix) {
    return new ToxiproxyContainer(TOXIPROXY_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(
            "proxy-instance-" + instance.getInstanceId(),
            instance.getHost() + proxyDomainNameSuffix);
  }

  // This container supports traces to AWS XRay.
  public GenericContainer<?> createTelemetryXrayContainer(
      String xrayAwsRegion,
      Network network,
      String networkAlias) {

    return new FixedExposedPortContainer<>(
        new ImageFromDockerfile("xray-daemon", true)
            .withDockerfileFromBuilder(
                builder -> builder
                        .from(XRAY_TELEMETRY_IMAGE_NAME)
                        .entryPoint("/xray",
                          "-t", "0.0.0.0:2000",
                          "-b", "0.0.0.0:2000",
                          "--local-mode",
                          "--log-level", "debug",
                          "--region", xrayAwsRegion)
                        .build()))
        .withExposedPort(2000)
        .waitingFor(Wait.forLogMessage(".*Starting proxy http server on 0.0.0.0:2000.*", 1))
        .withNetworkAliases(networkAlias)
        .withNetwork(network);
  }

  // This container supports traces and metrics to AWS CloudWatch/XRay
  public GenericContainer<?> createTelemetryOtlpContainer(
      Network network,
      String networkAlias) {

    return new FixedExposedPortContainer<>(DockerImageName.parse(OTLP_TELEMETRY_IMAGE_NAME))
        .withExposedPort(2000)
        .withExposedPort(1777)
        .withExposedPort(4317)
        .withExposedPort(4318)
        .waitingFor(Wait.forLogMessage(".*Everything is ready. Begin running and processing data.*", 1))
        .withNetworkAliases(networkAlias)
        .withNetwork(network)
        .withCopyFileToContainer(
            MountableFile.forHostPath("./src/test/resources/otel-config.yaml"),
            "/etc/otel-config.yaml");
  }

  public static class FixedExposedPortContainer<T extends FixedExposedPortContainer<T>> extends GenericContainer<T> {

    public FixedExposedPortContainer(ImageFromDockerfile withDockerfileFromBuilder) {
      super(withDockerfileFromBuilder);
    }

    public FixedExposedPortContainer(final DockerImageName dockerImageName) {
      super(dockerImageName);
    }

    public T withFixedExposedPort(int hostPort, int containerPort, InternetProtocol protocol) {
      super.addFixedExposedPort(hostPort, containerPort, protocol);
      return self();
    }

    public T withExposedPort(Integer port) {
      super.addExposedPort(port);
      return self();
    }
  }
}
