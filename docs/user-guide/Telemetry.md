# Telemetry

## Monitoring

Monitoring is the ability to gather data and insights on the execution of an application. Users will also be able to inspect the gathered data and determine potential actions to take depending on the data collected.

The Telemetry feature allows users to collect and visualize data of the AWS Advanced Go Wrapper execution at a global level and at a plugin level. You can use this feature to monitor the performance of the wrapper as a whole or within specific plugins with your configurations, and determine whether the wrapper's performance meets your expectations.

## Terminology

The AWS Advanced Go Wrapper provides telemetry data through two different forms: **Traces** and **Metrics**.

### Traces

Traces give an overview of what is happening in a specific section of the execution of an application. A trace is composed of a hierarchical sequence of segments, each of which contain basic information about the execution (e.g. duration), and whether that section was executed successfully.

In the AWS Advanced Go Wrapper, a trace will be generated for every call made to the wrapper. Depending on whether the user application already has a trace open, it may be nested in the open trace or dropped. And then, for each enabled plugin, another segment will be created only for the plugin execution, linked to the call segment.

Traces from the AWS Advanced Go Wrapper are submitted to [**AWS X-Ray**](https://aws.amazon.com/xray/).

### Metrics

Metrics are numeric data that was measured and collected through the execution of an application. Metrics can give insight on how many times an action (e.g. failover) has happened, and for actions that may happen multiple times, their success or failure rate (failover, cache hits, etc.), amongst other related information.

The AWS Advanced Go Wrapper will submit metrics data to [**Amazon CloudWatch**](https://aws.amazon.com/cloudwatch/).

The list of available metrics for the AWS Advanced Go Wrapper and its plugins is available in the [List of Metrics](#List-Of-Metrics) section of this page.

## Setting up the AWS Distro for OpenTelemetry Collector (ADOT Collector)

## Prerequisites

Before enabling the Telemetry feature, a few setup steps are required to ensure the monitoring data is properly emitted.

1. In order to visualize the telemetry data in the AWS Console, make sure you have an IAM user or role with permissions to [AWS X-Ray](https://docs.aws.amazon.com/xray/latest/devguide/security-iam.html) and [Amazon CloudWatch](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/auth-and-access-control-cw.html).

2. Download the [AWS Distro for OpenTelemetry Collector](https://aws-otel.github.io/docs/getting-started/collector) and set it up. The AWS Distro for OpenTelemetry Collector is responsible from receiving telemetry data from the application using the AWS Advanced Go Wrapper and forward it to AWS. Both of those connections happen via HTTP, therefore URLs and ports need to be correctly configured for the collector.

> [!WARNING]
> The AWS Distro for OpenTelemetry Collector can be set up either locally or remotely. It is up to the user to decide where is best to set it up. If you decide to host it remotely, ensure that the application has the necessary permissions or allowlists to connect to the Collector.

> [!WARNING]
> The collector is an external application that is not part of the wrapper itself. Without a collector, the wrapper will collect monitoring data from its execution but that data will not be sent anywhere for visualization.

## Using Telemetry

Telemetry for the AWS Advanced Go Wrapper is a monitoring strategy that overlooks all plugins enabled in [`plugins`](./UsingTheGoWrapper.md#connection-plugin-manager-parameters) and is not a plugin in itself. Changes are not required in the `plugins` parameter to enable Telemetry.

In order to enable Telemetry in the AWS Advanced Go Wrapper, you need to:

1. Set the `enableTelemetry` connection property to `true`.

2. Ensure the required wrapper modules for your chosen backend have been imported. The AWS Advanced Go Wrapper has two available backend options, `OTLP` and `XRAY`. This can be accomplished by running one of the following commands in the same directory as the intended `go.mod` file:
   - `go get github.com/aws/aws-advanced-go-wrapper/otlp`
   - `go get github.com/aws/aws-advanced-go-wrapper/xray`

3. Set up the recorders that will export the telemetry data from the code to the ADOT Collector.

Setting up the recorders require to instantiate an `OpenTelemetrySDK` in the application code prior to using the wrapper. Instantiating the `OpenTelemetrySDK` requires you to configure the endpoints where traces and metrics are being forwarded to.

The code samples below shows a simple manner to instantiate trace and metrics recording in an application using Telemetry. Please note that additional dependencies may be required to instantiate trace and metric recording.
[OTLP sample application](../../examples/telemetry_metrics_otlp_example.go)
[XRAY sample application](../../examples/telemetry_metrics_xray_example.go)

### Telemetry Parameters

In addition to the parameter that enables Telemetry, you can pass the following parameters to the AWS Advanced Go Wrapper to configure how telemetry data will be forwarded.

| Parameter                 |  Value  | Required | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Default Value |
|---------------------------|:-------:|:--------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `enableTelemetry`         | Boolean |    No    | Telemetry will be enabled when this property is set to `true`, otherwise no telemetry data will be gathered during the execution of the wrapper.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `false`       |
| `telemetryTracesBackend`  | String  |    No    | Determines to which backend the gathered tracing data will be forwarded to. Possible values include: `NONE`, `XRAY`, and `OTLP`.<br>`NONE` indicates that the application will collect tracing data but this data will not be forwarded anywhere.<br>`XRAY` indicates that the traces will be collected by the AWS XRay Daemon.<br>`OTLP` indicates that the traces will be collected by the AWS OTEL Collector. Please ensure the wrapper telemetry module of the traces backend has been imported. See [using telemetry](#using-telemetry) for more information.                                                                                                                       | `NONE`        |
| `telemetryMetricsBackend` | String  |    No    | Determines to which backend the gathered metrics data will be forwarded to. Possible values include: `NONE` and `OTLP`.<br>`NONE` indicates that the application will collect metrics data but this data will not be forwarded anywhere.<br>`OTLP` indicates that the metrics will be collected by the AWS OTEL Collector. Please ensure the wrapper telemetry module of the metrics backend has been imported. See [using telemetry](#using-telemetry) for more information.                                                                                                                                                                                                            | `NONE`        |
| `telemetrySubmitTopLevel` | Boolean |    No    | By default the wrapper will look for open traces in the users application prior to record telemetry data. If there is a current open trace, the traces created will be attached to that open trace. If not, all telemetry traces created will be top level. Setting the parameter to `false` means that every call to the wrapper will generate a trace with no direct parent trace attached to it. If there is already an open trace being recorded by the application, no wrapper traces will be created. See the [Nested tracing strategies section](#nested-tracing-strategies-using-amazon-xray) for more information.                                                              | `false`       |

## Nested tracing strategies using Amazon XRay

As you could see in the [Telemetry Parameters](#Telemetry-Parameters) section, the AWS Advanced Go Wrapper allows users to determine which strategy for nested traces to use when using Telemetry.

Traces are hierarchical entities and user application may already have an open trace in a given sequence of code that connects to the AWS Advanced Go Wrapper. In this case, the Telemetry feature allows users to determine which strategy to use for the Telemetry traces generated when using the wrapper.

A top level trace is a trace that has no link to any other parent trace, and is directly accessible from the list of submitted traces within XRay. In the following pictures, the top level traces of an application are displayed in AWS X-Ray.

<div style="text-align:center"><img src="../images/telemetry_top_level.png" /></div>

<div style="text-align:center"><img src="../images/telemetry_traces.png" /></div>

When a trace is hierarchically linked to a parent trace, we say that this trace is nested. An example of nested traces are the individual plugin traces for a given call. All the individual plugin traces are linked to a parent trace for the call. Those nested traces are illustrated in the image below.

<div style="text-align:center"><img src="../images/telemetry_nested.png" /></div>

Applications that interact with the AWS Advanced Go Wrapper may or may not have already opened telemetry traces on their own. In this case, it is up to the user to determine how they want to mix both application and wrapper traces.

This can be done using the AWS Advanced Go Wrapper's `telemetrySubmitTopLevel` property. This property allows users to choose to submit the wrapper traces always as top level traces or to submit them nested to the application traces.
The default value is set to `false`, which means the wrapper traces to always be nested into their application traces. That will happen unless there are no open application traces when the wrapper is running. In that case no wrapper traces will be collected or submitted. When the property is set to `true`, all the wrapper traces would be submitted top level traces. If the application has already open traces, it will not be possible for the wrapper traces to be top level and the wrapper traces therefore will not be submitted.

## List of Metrics

The AWS Advanced Go Wrapper also submits a set of metrics to Amazon CloudWatch when the wrapper is used. These metrics are predefined, and they help give insight on what is happening inside the plugins when the plugins are used.

Metrics can be one of 3 types: counters, gauges or histograms.

### EFM plugin

| Metric name                        | Metric type | Description                                                                                         |
|------------------------------------|-------------|-----------------------------------------------------------------------------------------------------|
| efm.connections.aborted            | Counter     | Number of times a connection was aborted after being defined as unhealthy by an EFM monitoring task |

### Secrets Manager plugin

| Metric name                           | Metric type | Description                                                   |
|---------------------------------------|-------------|---------------------------------------------------------------|
| secretsManager.fetchCredentials.count | Counter     | Number of times credentials were fetched from Secrets Manager |

### IAM plugin

| Metric name          | Metric type | Description                                  |
|----------------------|-------------|----------------------------------------------|
| iam.fetchToken.count | Counter     | Number of times tokens were fetched from IAM |

### Failover plugin

| Metric name                            | Metric type | Description                                                 |
|----------------------------------------|-------------|-------------------------------------------------------------|
| writerFailover.triggered.count         | Counter     | Number of times writer failover was triggered               |
| writerFailover.completed.success.count | Counter     | Number of times writer failover was completed and succeeded |
| writerFailover.completed.failed.count  | Counter     | Number of times writer failover was completed and failed    |
| readerFailover.triggered.count         | Counter     | Number of times reader failover was triggered               |
| readerFailover.completed.success.count | Counter     | Number of times reader failover was completed and succeeded |
| readerFailover.completed.failed.count  | Counter     | Number of times reader failover was completed and failed    |

### Stale DNS helper

| Metric name             | Metric type | Description                            |
|-------------------------|-------------|----------------------------------------|
| staleDNS.stale.detected | Counter     | Number of times DNS was detected stale |

### Federated Authentication plugin

| Metric name                    | Metric type | Description                                  |
|--------------------------------|-------------|----------------------------------------------|
| federatedAuth.fetchToken.count | Counter     | Number of times tokens were fetched from IAM |

### Okta plugin

| Metric name               | Metric type | Description                                  |
|---------------------------|-------------|----------------------------------------------|
| oktaAuth.fetchToken.count | Counter     | Number of times tokens were fetched from IAM |
