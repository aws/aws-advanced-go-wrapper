# Contributor Guide

### Setup

Clone the AWS Advanced Go Wrapper repository:

```bash
git clone https://github.com/aws/aws-advanced-go-wrapper.git
```

You can now make changes in the repository.

## Testing Overview

The AWS Advanced Go Wrapper uses the following tests to verify its correctness and performance:

| Tests                                         | Description                                                                                                           |
|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Unit tests                                    | Tests for AWS Advanced Go Wrapper correctness.                                                                        |
| Failover integration tests                    | Wrapper-specific tests for different reader and writer failover workflows using the Failover Connection Plugin.       |
| Enhanced failure monitoring integration tests | Wrapper-specific tests for the enhanced failure monitoring functionality using the Host Monitoring Connection Plugin. |
| AWS authentication integration tests          | Wrapper-specific tests for AWS authentication methods with the AWS IAM Authentication Plugin.                         |
| Connection plugin manager benchmarks          | The benchmarks measure the overhead from executing method calls with multiple connection plugins enabled.             |

### Performance Tests

### Running the Tests

Unit tests can be run from the `.test` module.

```bash
cd .test
go test ./test/...
```

#### Integration Tests

For more information on how to run the integration tests, please visit [Integration Tests](./IntegrationTests.md).

#### Sample Code

[Postgres Connection Test Sample Code](./../../examples/aws_simple_connection_postgresql_example.go)<br>
[MySQL Connection Test Sample Code](./../../examples/aws_simple_connection_mysql_example.go)

## Architecture

For more information on how the AWS Advanced Go Wrapper functions and how it is structured, please visit [Architecture](./Architecture.md).
