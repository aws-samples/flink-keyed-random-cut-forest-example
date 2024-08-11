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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import software.amazon.flink.example.rcf.data.InputDataGeneratorFunction;
import software.amazon.flink.example.rcf.model.InputData;
import software.amazon.flink.example.rcf.model.OutputData;
import software.amazon.flink.example.rcf.operator.KeyRandomCutForestOperator;
import software.amazon.flink.example.rcf.operator.RcfModelParams;
import software.amazon.flink.example.rcf.operator.RcfModelsConfig;

public class DataStreamJob {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(DataStreamJob.class);

    private static final int DEFAULT_SHINGLE_SIZE = 1;
    private static final int DEFAULT_SAMPLE_SIZE = 256;
    private static final int DEFAULT_NUMBER_OF_TREES = 50;
    private static final int DEFAULT_OUTPUT_AFTER = 512;


    private static final float ANOMALY_THRESHOLD = 0.8f;


    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO Make configurable
        long modelStateSaveIntervalMillis = 5000;
        long modelStateSaveTimerJitterMillis = 500;

        // Local dev specific settings
        if (isLocal(env)) {
            // Checkpointing and parallelism are set by Amazon Managed Service for Apache Flink when running on AWS
            env.enableCheckpointing(30000);
            env.setParallelism(2);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        }

        // Initialize the data generator source
        DataGeneratorSource<InputData> source = new DataGeneratorSource<>(
                new InputDataGeneratorFunction(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                TypeInformation.of(InputData.class));
        DataStream<InputData> inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data generator").uid("data-gen");

        // RCF model parameters. We only define the default that will be used for every key
        // TODO fetch parameters for each key from application configuration
        RcfModelsConfig modelsConfig = new RcfModelsConfig(
                RcfModelParams.builder()
                        .dimensions(InputDataGeneratorFunction.DIMENSIONS)
                        .shingleSize(DEFAULT_SHINGLE_SIZE)
                        .sampleSize(DEFAULT_SAMPLE_SIZE)
                        .numberOfTrees(DEFAULT_NUMBER_OF_TREES)
                        .outputAfter(DEFAULT_OUTPUT_AFTER)
                        .build());

        // Initialize the RCF operator
        KeyRandomCutForestOperator<InputData, OutputData> rcfOperator = new KeyRandomCutForestOperator<>(
                new InputDataMapper(),
                new ResultMapper(),
                modelsConfig,
                modelStateSaveIntervalMillis,
                modelStateSaveTimerJitterMillis);

        inputStream
                .keyBy(InputData::getKey)
                // RCF operator
                .process(rcfOperator).returns(OutputData.class).uid("rcf")
                // Pass-through map counting records and anomalies
                .map(new NoOpMapMetricEmittingFunction(ANOMALY_THRESHOLD, "RCF")).uid("counter")
                .print();


        // Execute program, beginning computation.
        env.execute();
    }


}
