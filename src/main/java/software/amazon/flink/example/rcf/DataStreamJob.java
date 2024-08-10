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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import software.amazon.flink.example.rcf.operator.KeyRandomCutForestOperator;
import software.amazon.flink.example.rcf.operator.RcfInputMapper;
import software.amazon.flink.example.rcf.operator.RcfResultMapper;

import java.util.Arrays;

public class DataStreamJob {

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

        GeneratorFunction<Long, InputRecord> ramdomInputRecordGeneratorFunction = index -> {
            String key = RandomStringUtils.randomAlphabetic(1).toUpperCase();
            float[] values = {RandomUtils.nextFloat(0f, 1.0f), RandomUtils.nextFloat(0f, 1.0f), RandomUtils.nextFloat(0f, 1.0f)};
            return new InputRecord(key, values);
        };

        DataGeneratorSource<InputRecord> source = new DataGeneratorSource<>(
                ramdomInputRecordGeneratorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(InputRecord.class));
        DataStream<InputRecord> inputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Data generator").uid("data-gen");

        RcfInputMapper<InputRecord> inputMapper = inputRecord -> Arrays.copyOf(inputRecord.getValues(), inputRecord.getValues().length);

        RcfResultMapper<InputRecord, String> resultMapper = (inputRecord, score) -> String.format("%s: %f", inputRecord.key, score);

        KeyRandomCutForestOperator<InputRecord, String> rcfOperator = new KeyRandomCutForestOperator<>(
                inputMapper,
                resultMapper,
                modelStateSaveIntervalMillis,
                modelStateSaveTimerJitterMillis);

        inputStream
                .keyBy(InputRecord::getKey)
                .process(rcfOperator).returns(String.class).uid("rcf")
                .print();


        // Execute program, beginning computation.
        env.execute();
    }


}
