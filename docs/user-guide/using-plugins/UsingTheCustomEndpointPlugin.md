# Custom Endpoint Plugin

The Custom Endpoint Plugin adds support for [RDS custom endpoints](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-custom-endpoint-creating.html). When the Custom Endpoint Plugin is in use, the driver will analyse custom endpoint information to ensure instances used in connections are part of the custom endpoint being used. This includes connections used in failover and read-write splitting.

## Dependencies

The plugin lives in its own module. Add it and import it for its side effects, so the plugin registers itself:

```bash
go get github.com/aws/aws-advanced-go-wrapper/custom-endpoint
```

```go
import (
    _ "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
)
```

Without that import the driver returns an unknown-plugin-code error when `customEndpoint` appears in `plugins`. The required AWS SDK modules are added as indirect dependencies.

## Required IAM permission

> [!IMPORTANT]
> The credentials the wrapper resolves must be allowed to call **`rds:DescribeDBClusterEndpoints`**. This applies **regardless of how your application authenticates to the database** — the plugin always calls the RDS control plane, whether or not the IAM Authentication Plugin is enabled.
>
> If the permission is missing, every connection attempt, every statement and every transaction commit to that endpoint fails after `waitForCustomEndpointInfoTimeoutMs` (default 5000 ms), for as long as the permission is absent. Recovery is automatic once the policy is attached, within 5 minutes, with no restart required.
>
> `Tx.Rollback`, `Conn.IsValid` and `Conn.ResetSession` are deliberately **not** gated on endpoint information, so an application can always abort a transaction and the connection pool keeps working normally. None of the three selects a host, so gating them could not prevent a connection from reaching an instance outside the endpoint - it would only prevent a rollback from reaching the server and add the wait to every pool checkout and release.

### Mitigating an incident without a code change

Both of these are DSN or property changes, so neither needs a rebuild:

* Set **`waitForCustomEndpointInfo=false`** to stop connections and statements failing while endpoint information is unavailable. Filtering still applies whenever information *is* available, so you keep the feature; the cost is that connections may reach instances outside the custom endpoint during the window before the first successful fetch.
* Remove **`customEndpoint`** from `plugins` to disable the feature entirely.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "rds:DescribeDBClusterEndpoints",
      "Resource": "*"
    }
  ]
}
```

Verify with the same credentials your application uses:

```bash
aws rds describe-db-cluster-endpoints --db-cluster-endpoint-identifier <your-custom-endpoint-id>
```

## How to use the Custom Endpoint Plugin with the AWS Advanced Go Wrapper

### Enabling the Custom Endpoint Plugin

1. If needed, create a custom endpoint using the AWS RDS Console:
    - If needed, review the documentation about [creating a custom endpoint](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-custom-endpoint-creating.html).
2. Add the plugin code `customEndpoint` to the [`plugins`](../UsingTheGoWrapper.md#connection-plugin-manager-parameters) value.
    - `plugins` **replaces** the default plugin list rather than adding to it. Setting `plugins=customEndpoint` alone therefore disables `failover` and `efm`, and leaves nothing that consumes the filtered host list. Include the plugins you want, for example `plugins=auroraConnectionTracker,customEndpoint,failover,efm`.
3. If you are using the failover plugin, set the failover parameter `failoverMode` according to the custom endpoint type. For example, if the custom endpoint you are using is of type `READER`, you can set `failoverMode` to `strict-reader`, or if it is of type `ANY`, you can set `failoverMode` to `reader-or-writer`.
4. Specify parameters that are required or specific to your case.

### Custom Endpoint Plugin Parameters

| Parameter                                    |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                          | Default Value         | Example Value |
|----------------------------------------------|:-------:|:--------:|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|---------------|
| `customEndpointRegion`                       | String  |    No    | The region of the cluster's custom endpoints. If not specified, the region will be parsed from the URL.                                                                                                                                                                                                                               | `""`                  | `us-west-1`   |
| `customEndpointInfoRefreshRateMs`            | Integer |    No    | Controls how frequently custom endpoint monitors fetch custom endpoint info, in milliseconds. A non-positive value is replaced by the default.                                                                                                                                                                                        | `30000`               | `20000`       |
| `customEndpointMonitorExpirationMs`          | Integer |    No    | Controls how long a monitor should run without use before expiring and being removed, in milliseconds.                                                                                                                                                                                                                                | `900000` (15 minutes) | `600000`      |
| `waitForCustomEndpointInfo`                  | Boolean |    No    | Controls whether to wait for custom endpoint info to become available before connecting or executing a method. Waiting is only necessary if a connection to a given custom endpoint has not been opened or used recently. Note that disabling this may result in occasional connections to instances outside of the custom endpoint. | `true`                | `true`        |
| `waitForCustomEndpointInfoTimeoutMs`         | Integer |    No    | Controls the maximum amount of time that the plugin will wait for custom endpoint info to be made available by the custom endpoint monitor, in milliseconds.                                                                                                                                                                          | `5000`                | `7000`        |

## What membership filtering does

While the plugin is enabled, the driver restricts the hosts it will choose for itself to the members of
the custom endpoint, as reported by `rds:DescribeDBClusterEndpoints`. This covers failover and
read/write splitting.

> [!NOTE]
> `auroraInitialConnectionStrategy` is **not** covered. It engages only for a cluster endpoint
> (`.cluster-`), not a custom endpoint (`.cluster-custom-`), so it does not resolve an instance behind a
> custom endpoint at all - filtered or otherwise.

The member list comes from the endpoint's own configuration, in one of two forms:

| Member list   | Meaning                                                                          |
|---------------|----------------------------------------------------------------------------------|
| **static**    | Only the listed instances are used, a writer included if it is listed.           |
| **exclusion** | Every instance in the cluster is used except the listed ones.                     |

Two consequences:

- The member list comes from AWS-side data, not from your configuration, so **changing the endpoint's
  member list changes your driver's routing without a deploy**, within one
  `customEndpointInfoRefreshRateMs` interval.
- An exclusion list that names the writer leaves the driver with **no writer to select**. Reader failover,
  and switching out of read-only mode with `awsctx.SetReadOnly`, then have no valid target and return an
  error. If that is not what you want, do not exclude the writer.

> [!NOTE]
> The endpoint's *type* (`READER`, `WRITER`, `ANY`) is not currently enforced - only its member list is.
> A writer that is a member of a `READER`-type endpoint remains eligible.

## Use IAM authentication with the Custom Endpoint Plugin

The `rds:DescribeDBClusterEndpoints` permission described [above](#required-iam-permission) is required whether or not you use IAM authentication. When you *do* use IAM authentication, make sure the same credentials also carry the `rds-db:connect` permission your cluster requires.
