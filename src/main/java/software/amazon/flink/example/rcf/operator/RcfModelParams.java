package software.amazon.flink.example.rcf.operator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Properties;

/**
 * Configuration (hyper-parameters) of a single RCF model.
 *
 * TODO add any other parameters required
 */
@Getter @AllArgsConstructor @Builder @ToString
public class RcfModelParams implements Serializable {
    private final int dimensions;
    private final int shingleSize;
    private final int numberOfTrees;
    private final int sampleSize;
    private final int outputAfter;

    /**
     * Name of the property names used to populate a RcfModelParams from a Properties
     */
    public static final class PropertiesNames {
        public static final String DIMENSIONS = "dimensions";
        public static final String SHINGLE_SIZE = "shingle.size";
        public static final String NUMBER_OF_TREES = "number.of.trees";
        public static final String SAMPLE_SIZE = "sample.size";
        public static final String OUTPUT_AFTER = "output.after";
    }

    public static RcfModelParams fromProperties(Properties modelParamsProperties, String modelKey) {
        return RcfModelParams.builder()
                .dimensions(getMandatoryInt(modelParamsProperties, PropertiesNames.DIMENSIONS, modelKey))
                .shingleSize(getMandatoryInt(modelParamsProperties, PropertiesNames.SHINGLE_SIZE, modelKey))
                .numberOfTrees(getMandatoryInt(modelParamsProperties, PropertiesNames.NUMBER_OF_TREES, modelKey))
                .sampleSize(getMandatoryInt(modelParamsProperties, PropertiesNames.SAMPLE_SIZE, modelKey))
                .outputAfter(getMandatoryInt(modelParamsProperties, PropertiesNames.OUTPUT_AFTER, modelKey))
                .build();
    }



    private static int getMandatoryInt(Properties prop, String propertyName, String modelKey) {
        if (!prop.containsKey(propertyName)) {
            throw new IllegalArgumentException("Missing required property: " + propertyName + " for modelKey: " + modelKey);
        }
        String valueStr = prop.getProperty(propertyName);
        try {
            return Integer.parseInt(valueStr);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid value " + valueStr + " for property: " + propertyName + " for modelKey: " + modelKey, ex);
        }
    }
}
