package software.amazon.flink.example.rcf.operator;

import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration of multiple RCF Models, by model key.
 */
@ToString
public class RcfModelsConfig implements Serializable {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(RcfModelsConfig.class);

    /**
     * Map of model parameters, by model key
     */
    @Getter
    private final Map<String, RcfModelParams> modelParamsMap = new HashMap<>();

    /**
     * Default model parameters, returned if no parameter is defined for a specific key
     */
    @Getter
    private final RcfModelParams defaultModelParams;

    /**
     * Initialize config with default model parameters
     */
    RcfModelsConfig(RcfModelParams defaultModelParams) {
        this.defaultModelParams = defaultModelParams;
    }

    /**
     * Add model parameters for a given model key
     */
    void addModelParams(String modelKey, RcfModelParams modelParams) {
        modelParamsMap.put(modelKey, modelParams);
    }

    /**
     * Return the model parameters for a given model key
     * falling back to the Default Model Parameters, if no parameters are defined for the given key
     */
    public RcfModelParams getModelParams(String modelKey) {
        if (!modelParamsMap.containsKey(modelKey)) {
            LOG.debug("No parameters defined for modelKye: {}. Using default parameters", modelKey);
            return defaultModelParams;
        }
        return modelParamsMap.get(modelKey);
    }

}
