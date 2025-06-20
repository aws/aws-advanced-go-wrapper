# Federated Authentication (ADFS) Plugin

The Federated Authentication Plugin adds support for authentication via Federated Identity and then database access via
IAM.
Currently, Microsoft Active Directory Federation Services (AD FS) and Okta are supported. To see information on how to
configure and use Okta authentication, see [Using the Okta Authentication Plugin](./UsingTheOktaAuthPlugin.md).

## What is Federated Identity

Federated Identity allows users to use the same set of credentials to access multiple services or resources across
different organizations. This works by having Identity Providers (IdP) that manage and authenticate user credentials,
and Service Providers (SP) that are services or resources that can be internal, external, and/or belonging to various
organizations. Multiple SPs can establish trust relationships with a single IdP.

When a user wants access to a resource, it authenticates with the IdP. From this a security token generated and is
passed to the SP then grants access to said resource.
In the case of AD FS, the user signs into the AD FS sign in page. This generates a SAML Assertion which acts as a
security token. The user then passes the SAML Assertion to the SP when requesting access to resources. The SP verifies
the SAML Assertion and grants access to the user.

## Prerequisites

This plugin requires:

1. Valid AWS credentials, as it does not create or modify any ADFS or IAM resources. All permissions and policies must be
  correctly configured before using this plugin: 
   1. Enable AWS IAM database authentication on an existing database or create a new database with AWS IAM database
        authentication on the AWS RDS Console:
      - If needed, review the documentation
        about [IAM authentication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).
   2. Set up an IAM Identity Provider and IAM role. The IAM role should be using the IAM policy set up in step 1.
      - If needed, review the documentation
        about [creating IAM identity providers](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create.html).
        For AD FS, see the documentation
        about [creating IAM SAML identity providers](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_create_saml.html).
2. [github.com/aws/aws-advanced-go-wrapper/federated-auth](../../../federated-auth) to be a dependency in the project
   - This can be accomplished by running `go get github.com/aws/aws-advanced-go-wrapper/federated-auth` in the same
        directory as the intended `go.mod` file.

When the `federated-auth` module is added as a dependency, the required AWS modules will also be added as indirect
dependencies.

> [!NOTE]
> AWS IAM database authentication is needed to use the Federated Authentication Plugin. This is because after the plugin
> acquires the authentication token (ex. SAML Assertion in the case of AD FS), the authentication token is then used to
> acquire an AWS IAM token. The AWS IAM token is then subsequently used to access the database.

## Enabling the Federated Authentication Plugin

To enable the AWS Secrets Manager Plugin, add the plugin code `federatedAuth` to the [`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters)
   value. Then, specify parameters that are required or specific to your case.

### Connecting with the Go-MySQL Driver

When connecting through IAM with the Go-MySQL-Driver, the additional parameter `allowCleartextPasswords=true` is required. By default, MySQL encrypts the password and when AWS receives it for IAM authentication it doesn't decrypt it and is unable to connect. When `allowCleartextPasswords` is set to `true` AWS receives the password as-is and is able to connect.

Additional case-specific configuration can be handled by registering a tls.Config with the underlying driver. See [MySQL IAM Sample Code](../../../examples/iam_mysql_example.go) for an example.

## Federated Authentication Plugin Parameters

| Parameter               |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                        | Default Value            | Example Value                                          |
|-------------------------|:-------:|:--------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|--------------------------------------------------------|
| `dbUser`                | String  |   Yes    | The user name of the IAM user with access to your database. <br>If you have previously used the IAM Authentication Plugin, this would be the same IAM user. <br>For information on how to connect to your Aurora Database with IAM, see this [documentation](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.Connecting.html). | `nil`                    | `some_user_name`                                       |
| `idpUsername`           | String  |   Yes    | The user name for the `idpEndpoint` server. If this parameter is not specified, the plugin will fallback to using the `user` parameter.                                                                                                                                                                                                                            | `nil`                    | `jimbob@example.com`                                   |
| `idpPassword`           | String  |   Yes    | The password associated with the `idpEndpoint` username. If this parameter is not specified, the plugin will fallback to using the `password` parameter.                                                                                                                                                                                                           | `nil`                    | `someRandomPassword`                                   |
| `idpEndpoint`           | String  |   Yes    | The hosting URL for the service that you are using to authenticate into AWS Aurora.                                                                                                                                                                                                                                                                                | `nil`                    | `ec2amaz-ab3cdef.example.com`                          |
| `iamRoleArn`            | String  |   Yes    | The ARN of the IAM Role that is to be assumed to access AWS Aurora.                                                                                                                                                                                                                                                                                                | `nil`                    | `arn:aws:iam::123456789012:role/adfs_example_iam_role` |
| `iamIdpArn`             | String  |   Yes    | The ARN of the Identity Provider.                                                                                                                                                                                                                                                                                                                                  | `nil`                    | `arn:aws:iam::123456789012:saml-provider/adfs_example` |
| `iamRegion`             | String  |   Yes    | The IAM region where the IAM token is generated.                                                                                                                                                                                                                                                                                                                   | `nil`                    | `us-east-2`                                            |
| `idpPort`               | String  |    No    | The port that the host for the authentication service listens at.                                                                                                                                                                                                                                                                                                  | `443`                    | `1234`                                                 |
| `rpIdentifier`          | String  |    No    | The relaying party identifier.                                                                                                                                                                                                                                                                                                                                     | `urn:amazon:webservices` | `urn:amazon:webservices`                               |
| `iamHost`               | String  |    No    | Overrides the host that is used to generate the IAM token.                                                                                                                                                                                                                                                                                                         | `nil`                    | `database.cluster-hash.us-east-1.rds.amazonaws.com`    |
| `iamDefaultPort`        | Integer |    No    | This property overrides the default port that is used to generate the IAM token. The default port is determined based on the underlying driver protocol.                                                                                                                                                                                                           | `nil`                    | `1234`                                                 |
| `iamTokenExpirationSec` | Integer |    No    | Overrides the default IAM token cache expiration. Value is in seconds.                                                                                                                                                                                                                                                                                             | `870`                    | `123`                                                  |
| `httpTimeoutMs`         | Integer |    No    | The timeout value in milliseconds provided to http clients used by the Federated Authentication Plugin. The default expiration time is set to be 14 minutes and 30 seconds. Note that IAM database authentication tokens have a lifetime of 15 minutes.                                                                                                            | `60000`                  | `60000`                                                |
| `sslInsecure`           | Boolean |    No    | Indicates whether or not the SSL connection is secure or not. If not, it will allow SSL connections to be made without validating the server's certificates. **Note**: This is useful for local testing, but setting this to true is not recommended for production environments.                                                                                  | `false`                  | `true`                                                 |

## Sample code

[MySQL Example](../../../examples/adfs_mysql_example.go), [PostgreSQL Example](../../../examples/adfs_postgres_example.go).
