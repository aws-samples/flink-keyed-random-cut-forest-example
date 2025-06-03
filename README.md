## Keyed implementation of Flink Random-Cut Forest

**This code example is for demonstration purposes only. The code should not be used for production workloads.**

This examples demonstrate an implmenetaion of stateful and multi-tenant anomaly detection based on Random Cut Forest (RCF). 
Each tenant has a dedicated RCF model. The state of the models is preserved in Flink state and survive job crashes and restarts.

In a multi-tenant system, when tenants have different traffic profiles, you need to use model-per-tenant for reliable results.

* Flink version: 1.20
* Flink API: DataStream API
* Language: Java (11)
* Flink connectors: Data Generator, Kinesis Sink

The example is designed to run on [Amazon Managed Service for Apache Flink](https://aws.amazon.com/managed-service-apache-flink/), 
but it can be easily adapted to any other Apache Flink distribution.
The only external dependency is an [Amazon Kinesis Data Stream](https://aws.amazon.com/kinesis/) where scored data are emitted.

This example is based on [this other Flink RCF implementation](https://github.com/aws-samples/amazon-kinesis-data-analytics-examples/tree/master/AnomalyDetection/RandomCutForest) (see also [this blog](https://aws.amazon.com/blogs/big-data/real-time-anomaly-detection-via-random-cut-forest-in-amazon-managed-service-for-apache-flink/)) with the relevant difference 
that input records are "keyed" (partitioned), input features are scored by and are used to train the model dedicated to that key.


This approach has several implications:
* Incoming events from each tenant (key) are scored independently
* Incoming events from each tenant (key) are processed by a single thread, in a single subtask (RCF is not designed to be distributed)
* Implementation has an additional level of complexity, because RCF state is not serializable  and cannot be directly put in Flink state.
  See [Stateful RCF implementation](#stateful-rcf-implementation) for more details.

## Stateful RCF implementation

### The challenge

The RCF model (`RandomCutForest`) is stateful by definition, because it holds the "forest" based on input data.

However, `RandomCutForest` is not serializable and cannot be put in Flink state directly.

A serializable form of the model state, `RandomCutForestState`, can be extracted using `RandomCutForestMapper.toState(rcfModel)`. 
However, this operation is expensive. Extracting the state on every processed record may have a performance impact.

The non-keyed example solves this problem implementing `CheckpointedFunction`, extracting the serializable state in 
`snapshotState(..)` only when Flink takes a checkpoint or savepoint, and restore the state in `initializeState(...)` when
Flink restarts from checkpoint/savepoint.  Unfortunately, `CheckpointedFunction` does not work with keyed state.

### The solution

The solution adopted in this example is based on the following:

* Keep the stateful RCF model `RandomCutForest` in memory, in a Map by key. Note that each instance of the operator 
  (each subtask) with have only the keys assigned to that subtask
* Initialize `RandomCutForest` lazily, when the first record of that key is received
* Use Timers to periodically extract the serializable state from `RandomCutForest` and put they in (keyed) `ValueState`

Note: because timers saving the state are not synchronized with checkpointing, the state of the RCF model is not strictly *exactly-once*.
However, because RCF is a statistical model, dropping a few datapoints will not change the scoring of the model in a relevant
way. The interval between saving the state of each RCF model (`model.state.save.interval.ms`) should be lower than the 
Checkpoint Interval, to minimize the datapoint loss in case of restart, but not too frequent, to minimize the overhead 
of extracting the serializable state.

## Deploying the example

### Prerequisites

The only external prerequisite is a Kinesis Data Stream to send the output, and an S3 bucket to upload the application JAR.

The Amazon Managed Service for Apache Flink application IAM Role must have sufficient permissions to write to the Kinesis stream.

### Deploy on Managed Service for Apache Flink

1. Create a Kinesis Data following [these instructions](https://docs.aws.amazon.com/streams/latest/dev/how-do-i-create-a-stream.html)
2. Build the Java application with `mvn package`. This generates a JAR file called `keyed-rcf-example_1.20.0-1.0.jar`
3. Upload the JAR file into an S3 bucket
4. Create a Managed Service for Apache Flink application, following [these instructions](https://docs.aws.amazon.com/managed-flink/latest/java/how-creating-apps.html).
   Configure the Runtime properties as explained in [Runtime confuguration](#runtime-configuration), below.
5. Edit the application IAM Role adding the *AmazonKinesisFullAccess* policy, to allow the application writing to the output stream.
6. Start the application
7. Observe the using the *Data viewer* in the Kinesis console.

> To apply the principle of least privilege, only assign the following permissions to the Kinesis Data Stream:
> `kinesis:DescribeStreamSummary`, `kinesis:ListShards`, `kinesis:PutRecord`, and `kinesis:PutRecords`. 
> See [Example resource-based policies for Kinesis data streams](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html#kinesis-stream-sharing-iam-examples)
> in Kinesis Data Streams documentation.

### Run the application locally, in your IDE

You can run this example directly in any IDE supporting AWS Toolkit, such as [IntelliJ](https://aws.amazon.com/intellij/)
and [Visual Studio Code](https://aws.amazon.com/visualstudiocode/), without any local Flink cluster or local Flink installation.

1. Edit the application configuration used when running locally, in the [flink-application-properties-dev.json](./src/main/resources/flink-application-properties-dev.json) file 
   located in the `resources` folder, to match your configuration (e.g. stream name and region).
2. Ensure you have a valid AWS authentication profile on your machine, and you have permissions to write to the output
   Kinesis Data Stream.
3. Use the IDE run configuration to run the application with the correct AWS profile and to include *provided dependencies*.
   See this [additional documentation](https://github.com/aws-samples/amazon-managed-service-for-apache-flink-examples/blob/main/java/running-examples-locally.md)
   for more details.
4. Run the application in the IDE as any Java application.



> When running in the IDE, you can control logging with [log4j.properties](src/main/resources/log4j2.properties) in the 
> `resources` folder. This configuration is ignored when the application is deployed on Managed Flink, where logging is
> controlled via application configuration.


## Runtime configuration

When running on Amazon Managed Service for Apache Flink the runtime configuration is read from Runtime Properties.
When running locally, the configuration is read from the [resources/flink-application-properties-dev.json](./src/main/resources/flink-application-properties-dev.json) file located in the resources folder.


This application allows to independently configure multiple components.

### Data generator

This component generates random data with phased sinusoidal patterns, randomly introducing outliers that should be detected
as "anomalies". Note that the actual input data are not really important. The goal is demonstrating how the Flink implementation
works, not to test RCF.

| Group ID  | Key                  | Description                            |
|-----------|----------------------|----------------------------------------|
| `DataGen` | `records.per.second` | Number of generated records per second |

### Output monitor

This component emits 3x metrics (exported to CloudWatch when running in Amazon Managed Service for Apache Flink):
* `processedRecordCount`: how many records have been processed
* `scoredRecordCount`: how many records got a score (> 0) from RCF
* `anomaliesCount`: how many records are scored above the configurable threshold (see below)

| Group ID        | Key                  | Description                   |
|-----------------|----------------------|-------------------------------|
| `OutputMonitor` | `ranomaly.threshold` | Score threshold for anomalies |


### RCF operator

This is the component implementing RCF. 
The only configuration parameters are related to the timers saving RCF state into Flink state.

| Group ID      | Key                            | Description                                                   |
|---------------|--------------------------------|---------------------------------------------------------------|
| `RcfOperator` | `model.state.save.interval.ms` | Interval between saving each RCF model state into Flink state |
| `RcfOperator` | `model.state.save.jitter.ms`   | Jitter introduced to the timer reschedule                     |

Note RCF model parameters (hyper)parameters are configured separately.

### RCF models parameters

RCF model parameters are configured by key.
A default is also configured, used if no specific configuration is provided for a key.

Note that in this example we only configure the following RCF parameters: `dimensions`, `shingle.size`, `sample.size`, `number.of.trees`, and `output.after`.
The example can easily be extended to make other parameters configurable.

See the [RCF implementation](https://github.com/aws/random-cut-forest-by-aws/tree/main/Java#forest-configuration) for a meaning of each parameter,
and to know other available parameters.

RCF Model parameters configuration properties Group ID have the format `ModelParameters-` followed by the value of the model key (as a string).
In this context "key" is the key used for the keyed-stream processed by the RCF operator (the key used in the `keyBy()` 
before the RCF operator).

The default parameters are in the Group ID `ModelParameters-DEFAULT`.

| Group ID                     | Key               | Description                                            |
|------------------------------|-------------------|--------------------------------------------------------|
| `ModelParameters-<modelKey>` | `dimensions`      | Dimensions                                             |
| `ModelParameters-<modelKey>` | `shingle.size`    | Shingle size                                           |
| `ModelParameters-<modelKey>` | `sample.size`     | Sample size                                            |
| `ModelParameters-<modelKey>` | `number.of.trees` | Number of trees size                                   |
| `ModelParameters-<modelKey>` | `output.after`    | Number of input samples before the model start scoring |


Note that, in the current implementation, if the Property Group for a given model-key is defined, all the parameters
must be defined. 
The RCF model is **either** configured with the key-specific configuration, **or** with the default configuration. They
are not merged.

> The example can be extended allowing key-specific configuration to only override parameters that are different from 
> the default.

### Output stream

The example emits scored records to a Kinesis Data Stream, as JSON. 
The output can be inspected in the AWS Console using the Kinesis Data Viewer.


| Group ID        | Key           | Description                  |
|-----------------|---------------|------------------------------|
| `OutputStream0` | `stream.name` | Name of the Kinesis stream   |
| `OutputStream0` | `aws.region`  | Region of the Kinesis stream |


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
