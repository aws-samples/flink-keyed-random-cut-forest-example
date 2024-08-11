package software.amazon.flink.example.rcf.data;


import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.slf4j.Logger;
import software.amazon.flink.example.rcf.model.InputData;

import java.time.Instant;

/**
 * Generates InputData.
 * Each dimension is a sine wave with fixed but different amplitude, and phased.
 * The generator randomly introduces an anomaly multiplying the value/
 */
public class InputDataGeneratorFunction implements GeneratorFunction<Long, InputData> {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(InputDataGeneratorFunction.class);


    public static final String[] KEYS = {"k1", "k2", "k3", "k4", "k5"};

    public static final float[] AMPLITUDE = {10f, 5f, 25f};
    public static final double[] PHASE = {0, Math.PI / 2, Math.PI / 4};

    public static final int DIMENSIONS = AMPLITUDE.length;
    public static final int PERIOD = 100; // In number of samples

    public static final double ANOMALY_PROB = 0.005;
    public static final double ANOMALY_MAGNITUDE = 50f;


    /**
     * Generates sinusoidal data points, phased bases on the dimension, with random anomalies.
     */
    private static float datapoint(long seq, int dimension) {
        double rad = 2.0 * Math.PI * seq / PERIOD + PHASE[dimension];
        double value = Math.sin(rad) * AMPLITUDE[dimension];

        double random = RandomUtils.nextDouble(0, 1.0);
        if (random < ANOMALY_PROB) {
            value *= RandomUtils.nextDouble(ANOMALY_MAGNITUDE / 2, ANOMALY_MAGNITUDE);
            LOG.debug("Anomaly generated! {}", value);
        }
        return (float) value;
    }

    private static float[] values(long seq) {
        float[] values = new float[DIMENSIONS];
        for (int i = 0; i < DIMENSIONS; i++) {
            values[i] = datapoint(seq, i);
        }
        return values;
    }

    private static String randomKey() {
        return KEYS[RandomUtils.nextInt(0, KEYS.length)];
    }

    @Override
    public InputData map(Long sequence) {
        return new InputData(
                Instant.now().toEpochMilli(),
                randomKey(),
                values(sequence));
    }


}
