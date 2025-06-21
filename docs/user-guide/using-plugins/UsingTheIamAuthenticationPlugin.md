# Iam Authentication Plugin

AWS Identity and Access Management (IAM) grants users access control across all Amazon Web Services. IAM supports
granular permissions, giving you the ability to grant different permissions to different users. For more information on
IAM and its use cases, please refer to
the [IAM documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html).

## Prerequisites

This plugin requires:

1. Valid AWS credentials: 
    1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database
       authentication on the AWS RDS Console:
       - If needed, review the documentation about [creating a new database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html).
       - If needed, review the documentation about [modifying an existing database](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html).
    2. Set up an [AWS IAM policy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html) for AWS IAM database authentication.
    3. [Create a database account](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html) using AWS IAM database authentication. This will be the user specified in the connection string or connection
       properties.
       - Connect to your database of choice using primary logins.
            1. For a MySQL database, use the following command to create a new user:<br>
               `CREATE USER example_user_name IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';`
            2. For a PostgreSQL database, use the following command to create a new user:<br>
               `CREATE USER db_userx;
               GRANT rds_iam TO db_userx;`
2. [github.com/aws/aws-advanced-go-wrapper/iam](../../../iam) to be a dependency in the project
    - This can be accomplished by running `go get github.com/aws/aws-advanced-go-wrapper/iam` in the same directory as
       the intended `go.mod` file.

When the `iam` module is added as a dependency, the required AWS modules will also be added as indirect dependencies.

## Enabling the IAM Authentication Plugin

To enable the IAM Authentication Plugin, add the plugin code `iam` to the [
`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) parameter value. Then, specify parameters that are required or specific to your case.

### Connecting with the Go-MySQL Driver

When connecting through IAM with the Go-MySQL-Driver, the additional parameter `allowCleartextPasswords=true` is required. By default, MySQL encrypts the password and when AWS receives it for IAM authentication it doesn't decrypt it and is unable to connect. When `allowCleartextPasswords` is set to `true` AWS receives the password as-is and is able to connect.

Additional case-specific configuration can be handled by registering a tls.Config with the underlying driver. See [MySQL IAM Sample Code](../../../examples/iam_mysql_example.go) for an example.

## IAM Authentication Plugin Parameters

| Parameter          |  Value  | Required | Description                                                                                                                                                                                                                                              | Example Value                                       |
|--------------------|:-------:|:--------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| `iamDefaultPort`   | String  |    No    | This property will override the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol.                                                                                             | `1234`                                              |
| `iamHost`          | String  |    No    | This property will override the default hostname that is used to generate the IAM token. The default hostname is derived from the connection string.                                                                                                     | `database.cluster-hash.us-east-1.rds.amazonaws.com` |
| `iamRegion`        | String  |    No    | This property will override the default region that is used to generate the IAM token. The default region is parsed from the connection string.                                                                                                          | `us-east-2`                                         |
| `iamExpirationSec` | Integer |    No    | This property determines how long an IAM token is kept in the cache before a new one is generated. The default expiration time is set to be 14 minutes and 30 seconds. Note that IAM database authentication tokens have a lifetime of 15 minutes.       | `600`                                               |

## Sample code

[MySQL Example](../../../examples/iam_mysql_example.go), [PostgreSQL Example](../../../examples/iam_postgres_example.go).
