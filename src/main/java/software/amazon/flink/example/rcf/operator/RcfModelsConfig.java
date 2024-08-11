package software.amazon.flink.example.rcf.operator;

import lombok.ToString;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration of multiple RCF Models, by model key.
 * <p>
 * TODO add default parameters to use when no specific parameters are provided for a given model key
 */
@ToString
public class RcfModelsConfig implements Serializable {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(RcfModelsConfig.class);

    /**
     * Name of the property names used to populate a RcfModelParams from a Properties
     */
    public static final class PropertiesNames {
        public static final String DIMENSIONS = "dimensions";
        public static final String SHINGLE_SIZE = "shingleSize";
        public static final String NUMBER_OF_TREES = "numberOfTrees";
        public static final String SAMPLE_SIZE = "sampleSize";
        public static final String OUTPUT_AFTER = "outputAfter";
    }

    /**
     * Map of model parameters, by model key
     */
    private final Map<String, RcfModelParams> modelParamsMap = new HashMap<>();

    /**
     * Default model parameters, returned if no parameter is defined for a specific key
     */
    private final RcfModelParams defaultModelParams;

    public RcfModelsConfig(RcfModelParams defaultModelParams) {
        this.defaultModelParams = defaultModelParams;
    }

    /**
     * Add model parameters for a given model key
     */
    public RcfModelsConfig addModelParams(String modelKey, RcfModelParams modelParams) {
        modelParamsMap.put(modelKey, modelParams);
        return this;
    }

    /**
     * Add parameters, extracting from a Properties, for a given model key
     */
    public RcfModelsConfig addModelParams(String modelKey, Properties modelParamsProperties) {
        RcfModelParams modelParams = RcfModelParams.builder()
                .dimensions(getMandatoryInt(modelParamsProperties, PropertiesNames.DIMENSIONS, modelKey))
                .shingleSize(getMandatoryInt(modelParamsProperties, PropertiesNames.SHINGLE_SIZE, modelKey))
                .numberOfTrees(getMandatoryInt(modelParamsProperties, PropertiesNames.NUMBER_OF_TREES, modelKey))
                .sampleSize(getMandatoryInt(modelParamsProperties, PropertiesNames.SAMPLE_SIZE, modelKey))
                .outputAfter(getMandatoryInt(modelParamsProperties, PropertiesNames.OUTPUT_AFTER, modelKey))
                .build();
        return addModelParams(modelKey, modelParams);
    }

    private static int getMandatoryInt(Properties prop, String propertyName, String modelKey) {
        if (!prop.contains(propertyName)) {
            throw new IllegalArgumentException("Missing required property: " + propertyName + " for modelKey: " + modelKey);
        }
        String valueStr = prop.getProperty(modelKey);
        try {
            return Integer.parseInt(valueStr);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid value " + valueStr + " for property: " + propertyName + " for modelKey: " + modelKey, ex);
        }
    }

    /**
     * Return the model parameters for a given model key
     */
    public RcfModelParams getModelParams(String modelKey) {
        if (!modelParamsMap.containsKey(modelKey)) {
            LOG.debug("No parameters defined for modelKye: {}. Using default parameters", modelKey);
            return defaultModelParams;
        }
        return modelParamsMap.get(modelKey);
    }

}
