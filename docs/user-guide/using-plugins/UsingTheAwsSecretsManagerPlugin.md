# AWS Secrets Manager Plugin

The AWS Advanced Go Wrapper supports usage of database credentials stored as secrets in
the [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) through the AWS Secrets Manager Plugin. When you
create a new connection with this plugin enabled, the plugin will retrieve the secret and the connection will be created
with the credentials inside that secret.

## Prerequisites

This plugin requires:

1. Valid [AWS Secrets Manager credentials](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html)
2. [github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager](../../../aws-secrets-manager) to be a dependency in the
  project
   - This can be accomplished by running `go get github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager` in the same directory as
     the intended `go.mod` file.

   - The module registers the plugin when it is imported, so the application must import it for its side effects. Without the import, `awsSecretsManager` is an unknown plugin code.

     ```go
     import (
         _ "github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager"
     )
     ```

When the `aws-secrets-manager` module is added as a dependency, the required AWS modules will also be added as indirect
dependencies.

## Enabling the AWS Secrets Manager Plugin

To enable the AWS Secrets Manager Plugin, add the plugin code `awsSecretsManager` to the [
`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) value. Then specify the required parameters.

> [!WARNING]
> The `plugins` value should not contain more than one of the following codes: `awsSecretsManager`, `federatedAuth`, `iam`, and `okta` as each connection should use only one method of authentication.

## AWS Secrets Manager Plugin Parameters

The following properties are required for the AWS Secrets Manager Plugin to retrieve database credentials from the AWS
Secrets Manager.

| Parameter                              |  Value  |                         Required                         | Description                                                                                                                                                                                                                      | Example                 | Default Value |
|----------------------------------------|:-------:|:--------------------------------------------------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------------------------|---------------|
| `secretsManagerSecretId`               | String  |                           Yes                            | Set this value to be the secret name or the secret ARN.                                                                                                                                                                          | `secretId`              | `nil`         |
| `secretsManagerRegion`                 | String  | Yes unless the `secretsManagerSecretId` is a Secret ARN. | Set this value to be the region your secret is in. Leaving it unset selects the default rather than failing, so set it explicitly unless your secret is in `us-east-1`.                                                            | `us-east-2`             | `us-east-1`   |
| `secretsManagerEndpoint`               | String  |                            No                            | Set this value to be the endpoint override to retrieve your secret from. This parameter value should be in the form of a URL, with a valid protocol (ex. `http://`) and domain (ex. `localhost`). A port number is not required. | `http://localhost:1234` | `nil`         |
| `secretsManagerExpirationSec`          | Integer |                            No                            | This property sets the time in seconds that secrets are cached before it is re-fetched.                                                                                                                                          | `600`                   | `870`         |
| `secretsManagerSecretUsernameProperty` | String  |                            No                            | Set this value to be the key in the JSON secret that contains the username for database connection.                                                                                                                              | `db_user`               | `username`    |
| `secretsManagerSecretPasswordProperty` | String  |                            No                            | Set this value to be the key in the JSON secret that contains the password for database connection.                                                                                                                              | `db_pass`               | `password`    |
| `secretsManagerConnectRetryTimeoutMs`  | Integer |                            No                            | How long to keep retrying a connection that failed to log in, re-fetching the credentials before each retry. Set this to get past a [secret rotation window](#secret-rotation). `0` disables retrying.                            | `90000`                 | `0`           |
| `secretsManagerConnectRetryIntervalMs` | Integer |                            No                            | Initial delay before a failed connection is retried. The delay doubles after each failed attempt, capped at 30000ms and at the remaining budget. Only used when `secretsManagerConnectRetryTimeoutMs` is greater than `0`.          | `2000`                  | `1000`        |

> [!NOTE]
> A Secret ARN has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:SecretName-6RandomCharacters`.

## Secret Data

The secret stored in the AWS Secrets Manager should be a JSON object containing the properties `username` and `password`. If the secret contains different key names, you can specify them with the `secretsManagerSecretUsernameProperty` and `secretsManagerSecretPasswordProperty` parameters.

## Secret Rotation

A Secrets Manager rotation runs as `createSecret` → `setSecret` → `testSecret` → `finishSecret`. Between `setSecret`, which changes the database password, and `finishSecret`, which promotes `AWSCURRENT` to the new version, the database expects the new password while `GetSecretValue` still returns the old one. With RDS managed rotation this window has been reported at roughly a minute.

By default the plugin re-fetches the secret at most once when a connection fails to log in, so it cannot get through that window: the re-fetch resolves `AWSCURRENT` to the same old secret. Existing connections keep working, but every new physical connection fails for the duration, at startup, as the pool grows, and when `SetConnMaxLifetime` recycles a connection. The failure surfaces as a login error, `28P01` on PostgreSQL and `28000` on MySQL.

Setting `secretsManagerConnectRetryTimeoutMs` to a value longer than the window makes the plugin re-fetch the credentials and reconnect until it succeeds or the budget runs out:

```go
plugins := "awsSecretsManager"
connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s plugins=%s"+
	" secretsManagerSecretId=%s secretsManagerRegion=%s"+
	" secretsManagerConnectRetryTimeoutMs=90000 secretsManagerConnectRetryIntervalMs=2000",
	host, port, user, password, dbName, plugins, secretsManagerSecretId, secretsManagerRegion)

db, err := sql.Open("awssql-pgx", connStr)
```

Only login failures are retried. Any other error is reported immediately, since re-fetching the secret would not help.

Things to weigh before enabling it:

- Each retry issues one `GetSecretValue` call and re-resolves the AWS credential chain, so pick an interval with the Secrets Manager request quota in mind.
- The calling goroutine blocks while it waits, and a `context.Context` deadline cannot interrupt it. Keep the budget below whatever connect delay your application or connection pool tolerates.
- The budget bounds when the next attempt starts, not the total time spent, so the last attempt can finish after the budget has elapsed.

This is a client-side mitigation. [RDS Proxy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.html) or a [multi-user rotation strategy](https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets_strategies.html) avoids the window instead of waiting it out.

### Sample code

[MySQL Example](../../../examples/aws_secrets_manager_mysql_example.go), [PostgreSQL Example](../../../examples/aws_secrets_manager_postgres_example.go).
