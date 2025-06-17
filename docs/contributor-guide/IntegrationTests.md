# Integration Tests

### Prerequisites

Before running the integration tests for the AWS Advanced Go Wrapper, you must install:

- Docker Desktop:
  - [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
  - [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
  - [Docker Desktop for Linux](https://docs.docker.com/desktop/setup/install/linux/)
- Amazon Corretto 8+ or another Java 8+ JDK

#### Aurora Test Requirements

- An AWS account with:

  - RDS permissions.
  - EC2 permissions so integration tests can add the current IP address in the Aurora cluster's EC2 security group.
  - For more information, see: [Setting Up for Amazon RDS User Guide](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_SettingUp.html).

- An available Aurora PostgreSQL or MySQL DB cluster is required if you're running the tests against an existing DB cluster. The `REUSE_RDS_CLUSTER` [environment variable](#environment-variables-for-running-against-an-existing-aurora-cluster) is required to run tests against an existing cluster.
- An IAM user or role with permissions to AWS X-Ray and Amazon CloudWatch is required to visualize the telemetry data in the AWS Console. For more details, see: [Telemetry](../user-guide/Telemetry.md).

### Aurora Integration Tests

The Aurora integration tests are focused on testing connection strings and failover capabilities.
The tests are run in Docker but make a connection to test against an Aurora cluster. These tests can be run either:

1. against an existing cluster.
2. against new Aurora clusters that the tests spin up.

Both approaches will incur costs.
PostgreSQL and MySQL tests are currently supported.

> [!TIP]
> If you are not running against an existing cluster (`REUSE_RDS_CLUSTER` is `false`), the test will automatically create and delete the test resources. However, if the tests fail, the test resources may not be fully cleaned up. After running the integration tests, ensure all test resources are cleaned up.

#### Environment Variables

If the environment variable `REUSE_RDS_CLUSTER` is set to true, the integration tests will use the existing cluster defined by your environment variables. Otherwise, the integration tests will create a new Aurora cluster and then delete it automatically when the tests are done. Note that you will need a valid Docker environment to run any of the integration tests because they are run using a Docker environment as a host. The appropriate Docker containers will be created automatically when you run the tests, so you will not need to execute any Docker commands manually. If an environment variable listed in the tables below is not provided by the user, it may use a default value.

> [!NOTE]
> If you are running tests against an existing cluster, the tests will only run against the Aurora database engine of that cluster. For example, if you specify a MySQL cluster using the environment variables, only the MySQL tests will be run even if you pick test-all-aurora as the task. To run against PostgreSQL instead, you will need to change your environment variables.

##### Environment Variables for Running Against a New Aurora Cluster

| Environment Variable Name        | Description                                                                                                                                                                                                                      | Example Value                                |
|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------|
| `AWS_ACCESS_KEY_ID`              | An AWS access key associated with an IAM user or role with RDS permissions.                                                                                                                                                      | `ASIAIOSFODNN7EXAMPLE`                       |
| `AWS_SECRET_ACCESS_KEY`          | The secret key associated with the provided AWS_ACCESS_KEY_ID.                                                                                                                                                                   | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`   |
| `AWS_SESSION_TOKEN`              | AWS Session Token for CLI, SDK, & API access. This value is only required when using MFA credentials. See: [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html). | `AQoDYXdzEJr...<remainder of session token>` |
| `RDS_DB_REGION`                  | The database region.                                                                                                                                                                                                             | `us-east-1`                                  |
| `AURORA_MYSQL_DB_ENGINE_VERSION` | The MySQL database version to run against. You can specify a specific version like `8.0.mysql_aurora.3.04.0`, or use `default` or `latest`.                                                                                      | `default`                                    |
| `AURORA_PG_DB_ENGINE_VERSION`    | The PostgreSQL database version to run against. You can specify a specific version like `15.4`, or `default` or `latest`.                                                                                                        | `default`                                    |

###### (Optional) Additional Environment Variables

| Environment Variable Name | Description                                                                                                                        | Example Value       | Default Value (If available)                                                      |
|---------------------------|------------------------------------------------------------------------------------------------------------------------------------|---------------------|-----------------------------------------------------------------------------------|
| `DB_USERNAME`             | The username to access the database.                                                                                               | `admin`             | `test_user`                                                                       |
| `DB_PASSWORD`             | The database cluster password.                                                                                                     | `password`          | `secret_password`                                                                 |
| `DB_DATABASE_NAME`        | Name of the database that will be used by the tests. The default database name is test.                                            | `test_db_name`      | `test_database`                                                                   |
| `RDS_CLUSTER_NAME`        | The database identifier for your Aurora or RDS cluster. Must be a unique value to avoid conflicting with existing clusters.        | `db-identifier`     |
| `IAM_USER`                | User within the database that is identified with AWSAuthenticationPlugin. This is used for AWS IAM Authentication and is optional. | `example_user_name` | `jane_doe`                                                                        |
| `NUM_INSTANCES`           | The number of database instances in the cluster to test with. This value must be one of the following: `1`, `2`, `3`, `5`.         | `5`                 | Integration tests will be run several times, against `1`, `2`, and `5` instances. |

##### Environment Variables for Running Against an Existing Aurora Cluster

| Environment Variable Name | Description                                                                                                                                                                                                                      | Example Value                                | Default Value (If available) |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------|------------------------------|
| `DB_USERNAME`             | The database username to access the specified cluster.                                                                                                                                                                           | `admin`                                      | `test_user`                  |
| `DB_PASSWORD`             | The database password to access the specified cluster.                                                                                                                                                                           | `password`                                   | `secret_password`            |
| `DB_DATABASE_NAME`        | Name of the database that will be used by the tests.                                                                                                                                                                             | `test_db_name`                               | `test_database`              |
| `RDS_CLUSTER_NAME`        | The database identifier for your Aurora or RDS cluster. Must be a unique value to avoid conflicting with existing clusters.                                                                                                      | `db-identifier`                              |
| `RDS_CLUSTER_DOMAIN`      | The database connection suffix of the existing cluster.[^1]                                                                                                                                                                      | `XYZ.us-east-2.rds.amazonaws.com`            |
| `IAM_USER`                | User within the database that is identified with AWSAuthenticationPlugin. This is used for AWS IAM Authentication and is optional.                                                                                               | `example_user_name`                          | `jane_doe`                   |
| `AWS_ACCESS_KEY_ID`       | An AWS access key associated with an IAM user or role with RDS permissions.                                                                                                                                                      | `ASIAIOSFODNN7EXAMPLE`                       |
| `AWS_SECRET_ACCESS_KEY`   | The secret key associated with the provided AWS_ACCESS_KEY_ID.                                                                                                                                                                   | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`   |
| `AWS_SESSION_TOKEN`       | AWS Session Token for CLI, SDK, & API access. This value is only required when using MFA credentials. See: [temporary AWS credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html). | `AQoDYXdzEJr...<remainder of session token>` |
| `REUSE_RDS_CLUSTER`       | Must be set to true to use a specified existing cluster for tests. If you would like to have the tests create a cluster, see [here](#environment-variables-for-running-against-a-new-aurora-cluster).                            | `true`                                       | `false`                      |
| `RDS_DB_REGION`           | The database region.                                                                                                                                                                                                             | `us-east-1`                                  | `us-east-1`                  |

###### (Optional) Additional Environment Variables

| Environment Variable Name | Description                                                                                                                | Example Value | Default Value (If available)                                                      |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------|
| `NUM_INSTANCES`           | The number of database instances in the cluster to test with. This value must be one of the following: `1`, `2`, `3`, `5`. | `5`           | Integration tests will be run several times, against `1`, `2`, and `5` instances. |

### Standard Integration Tests

These integration tests are focused on testing connection strings against a local database inside a Docker container.
PostgreSQL and MySQL tests are currently supported.

### Available Integration Test Tasks

The following are the currently available integration test tasks. Each task may run a different subset of integration tests:

#### Standard Integration Test Tasks

- `test-docker`: run standard database tests

#### Aurora Integration Test Tasks

- `test-all-environments`: run all Aurora and standard database tests
- `test-aurora`: run Aurora tests
- `test-aurora-mysql`: run Aurora tests on the MySQL database type, use when running locally
- `test-aurora-postgres`: run Aurora tests on the PostgreSQL database type, use when running locally

### Running the Integration Tests

1. Ensure all [prerequisites](#prerequisites) have been installed. Docker Desktop must be running.
2. If you are running any Aurora integration tests, ensure the [Aurora Test Requirements](#aurora-test-requirements) have been met.
3. Set up [environment variables](#environment-variables).
4. Run one of the available [integration test tasks](#available-integration-test-tasks). For example, to run all integration tests, you can use the following commands:

macOS:

```bash
./gradlew --no-parallel --no-daemon test-all-environments
```

Windows:

```bash
cmd /c ./gradlew --no-parallel --no-daemon test-all-environments
```

Linux:

```bash
./gradlew --no-parallel --no-daemon test-all-environments
```

Test results can be found in `.test/test_framework/host/build/test-results/test-all-environments/`.

[^1]: The cluster domain suffix can be determined by checking the endpoint of an existing cluster in the desired region, or by temporarily creating a database to check the endpoint. For example, given the database endpoint `db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com`, the domain suffix would be `XYZ.us-east-2.rds.amazonaws.com`. See [here](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Endpoints.Cluster.html) for more information on Amazon Aurora cluster endpoints.
