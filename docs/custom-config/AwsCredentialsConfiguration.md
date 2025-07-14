# AWS Credentials Provider Configuration

### Applicable plugins: IamAuthenticationPlugin, AwsSecretsManagerPlugin

The `IamAuthenticationPlugin` and `AwsSecretsManagerPlugin` both require authentication via AWS credentials to provide the functionality they offer. In the plugin logic, the mechanism to locate your credentials is defined by passing in an `AwsCredentialsProvider` struct to the applicable AWS SDK client. By default, a default config will be passed, which locates your credentials using the default credential provider chain described [in this doc](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html#specifying-credentials). If AWS credentials are provided by the `credentials` and `config` files, then it's possible to specify a profile name using the `awsProfile` configuration parameter. If no profile name is specified, a `[default]` profile is used.

If you would like to use a different `AwsCredentialsProvider` or would like to define your own mechanism for providing AWS credentials, you can do so by creating a struct that implements the `AwsCredentialsProviderHandler` interface. To configure the plugins to use your own logic, you can call the `auth_helpers.SetAwsCredentialsProviderHandler` method and pass in the struct that implements `AwsCredentialsProviderHandler`.

## Sample code
- [Set custom credentials provider handler example](../../examples/aws_credentials_provider_handler_example.go)
- [Set aws profile example](../../examples/aws_profile_example.go)