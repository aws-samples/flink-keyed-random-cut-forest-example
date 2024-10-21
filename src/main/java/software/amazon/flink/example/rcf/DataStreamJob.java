/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.flink.example.rcf;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import software.amazon.flink.example.rcf.datagen.InputDataGeneratorFunction;
import software.amazon.flink.example.rcf.model.InputData;
import software.amazon.flink.example.rcf.model.OutputData;
import software.amazon.flink.example.rcf.monitor.NoOpMapOutputMonitorFunction;
import software.amazon.flink.example.rcf.operator.KeyRandomCutForestOperator;
import software.amazon.flink.example.rcf.operator.KeyRandomCutForestOperatorBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class DataStreamJob {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DataStreamJob.class);

    // Name of the local JSON resource with the application properties in the same format as they are received from the Amazon Managed Service for Apache Flink runtime
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

    /**
     * Load application properties from Amazon Managed Service for Apache Flink runtime or from a local resource, when the environment is local
     */
    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        if (isLocal(env)) {
            LOG.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            return KinesisAnalyticsRuntime.getApplicationProperties(
                    DataStreamJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath());
        } else {
            LOG.info("Loading application properties from Amazon Managed Service for Apache Flink");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }



    // Data generator, generating synthetic data
    private static DataGeneratorSource<InputData> buildSource(Properties sourceProperties) {
        double recordsPerSecond = ApplicationConfigUtils.parseMandatoryDouble(sourceProperties, "records.per.second");
        return new DataGeneratorSource<>(
                new InputDataGeneratorFunction(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                TypeInformation.of(InputData.class));
    }

    // The actual keyed RCF operator
    private static KeyRandomCutForestOperator<InputData, OutputData> buildRcfOperator(Map<String, Properties> applicationProperties) {
        return new KeyRandomCutForestOperatorBuilder<InputData,OutputData>()
                .parseOperatorProperties(applicationProperties.get("RcfOperator") )
                .setInputMapper(new InputDataMapper())
                .setResultMapper(new ResultMapper())
                .setDefaultModelParameters(applicationProperties, "ModelParameters-")
                .setAllSpecificModelParametersProperties(applicationProperties, "ModelParameters-")
                .build();
    }

    // This pass-through Map operator is used to expose custom metrics
    private static NoOpMapOutputMonitorFunction buildOutputMonitorFunction(Properties outputMonitorProperties) {
        float anomalyThreshold = ApplicationConfigUtils.parseMandatoryFloat(outputMonitorProperties, "anomaly.threshold");
        return new NoOpMapOutputMonitorFunction(anomalyThreshold, "kinesisAnalytics"); // This group makes Managed Flink to export the metric to CloudWatch Metrics
    }

    private static KinesisStreamsSink<OutputData> buildKinesisSink(Properties kinesisSinkProperties) {
        String outputStreamName = ApplicationConfigUtils.parseMandatoryString(kinesisSinkProperties, "stream.name");
        LOG.info("Initialize Kinesis sink to stream '{}'", outputStreamName);

        return KinesisStreamsSink.<OutputData>builder()
                .setStreamName(outputStreamName)
                // Any other Kinesis sink properties except stream name is passed to the sink builder
                .setKinesisClientProperties(kinesisSinkProperties)
                .setSerializationSchema(new JsonSerializationSchema<>())
                .setPartitionKeyGenerator(OutputData::getKey)
                .build();
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Load application parameters (runtime configuration)
        Map<String, Properties> applicationProperties = loadApplicationProperties(env);

        // Local dev specific settings
        if (isLocal(env)) {
            // Checkpointing and parallelism are set by Amazon Managed Service for Apache Flink when running on AWS
            env.enableCheckpointing(30000);
            env.setParallelism(2);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        }

        // Initialize the data generator source
        DataGeneratorSource<InputData> source = buildSource(applicationProperties.get("DataGen"));

        // Initialize the RCF operator
        KeyRandomCutForestOperator<InputData, OutputData> rcfOperator = buildRcfOperator(applicationProperties);

        // Initialize no-op record-emitting output monitor
        NoOpMapOutputMonitorFunction outputMonitorFunction = buildOutputMonitorFunction(applicationProperties.get("OutputMonitor"));

        // Initialize Kinesis Streams sink
        KinesisStreamsSink<OutputData> kinesisSink = buildKinesisSink(applicationProperties.get("OutputStream0"));

        // Defines the flow
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data generator").uid("data-gen")
                .keyBy(InputData::getKey)
                // RCF operator
                .process(rcfOperator).returns(OutputData.class).uid("rcf").name("RCF")
                // Pass-through map counting records and anomalies
                .map(outputMonitorFunction).uid("output-monitor").name("Output monitor")
//                .print()
                // Sink results to Kinesis
                .sinkTo(kinesisSink);

        // Execute program, beginning computation.
        env.execute("Keyed RCF");
    }
}
