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
| `secretsManagerRegion`                 | String  | Yes unless the `secretsManagerSecretId` is a Secret ARN. | Set this value to be the region your secret is in.                                                                                                                                                                               | `us-east-2`             | `nil`         |
| `secretsManagerEndpoint`               | String  |                            No                            | Set this value to be the endpoint override to retrieve your secret from. This parameter value should be in the form of a URL, with a valid protocol (ex. `http://`) and domain (ex. `localhost`). A port number is not required. | `http://localhost:1234` | `nil`         |
| `secretsManagerExpirationSec`          | Integer |                            No                            | This property sets the time in seconds that secrets are cached before it is re-fetched.                                                                                                                                          | `600`                   | `870`         |
| `secretsManagerSecretUsernameProperty` | String  |                            No                            | Set this value to be the key in the JSON secret that contains the username for database connection.                                                                                                                              | `db_user`               | `username`    |
| `secretsManagerSecretPasswordProperty` | String  |                            No                            | Set this value to be the key in the JSON secret that contains the password for database connection.                                                                                                                              | `db_pass`               | `password`    |

> [!NOTE]
> A Secret ARN has the following format: `arn:aws:secretsmanager:<Region>:<AccountId>:secret:SecretName-6RandomCharacters`.

## Secret Data

The secret stored in the AWS Secrets Manager should be a JSON object containing the properties `username` and `password`. If the secret contains different key names, you can specify them with the `secretsManagerSecretUsernameProperty` and `secretsManagerSecretPasswordProperty` parameters.

### Sample code

[MySQL Example](../../../examples/aws_secrets_manager_mysql_example.go), [PostgreSQL Example](../../../examples/aws_secrets_manager_postgres_example.go).
