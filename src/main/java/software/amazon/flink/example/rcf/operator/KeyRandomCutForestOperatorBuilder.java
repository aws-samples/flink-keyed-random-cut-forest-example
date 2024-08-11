package software.amazon.flink.example.rcf.operator;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.util.Preconditions;
import software.amazon.flink.example.rcf.ApplicationConfigUtils;

import java.util.Map;
import java.util.Properties;

import static software.amazon.flink.example.rcf.operator.KeyRandomCutForestOperatorBuilder.PropertiesNames.MODEL_STATE_SAVE_INTERVAL_MS;
import static software.amazon.flink.example.rcf.operator.KeyRandomCutForestOperatorBuilder.PropertiesNames.MODEL_STATE_SAVE_JITTER_MS;

public class KeyRandomCutForestOperatorBuilder<IN, OUT> {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(KeyRandomCutForestOperatorBuilder.class);


    // The default model parameters PropertyGroupId must have this suffix
    public static final String DEFAULT_MODEL_PARAMETERS_PROPERTY_GROUP_ID_SUFFIX = "DEFAULT";

    public static final class PropertiesNames {
        public static final String MODEL_STATE_SAVE_INTERVAL_MS = "model.state.save.interval.ms";
        public static final String MODEL_STATE_SAVE_JITTER_MS = "model.state.save.jitter.ms";
    }

    private static final long DEFAULT_MODEL_STATE_SAVE_INTERVAL_MILLIS = 10_000;
    private static final long DEFAULT_MODEL_STATE_SAVE_JITTER_MILLIS = 0;


    private RcfModelsConfig modelsConfig;
    private RcfInputMapper<IN> inputMapper;
    private RcfResultMapper<IN, OUT> resultMapper;

    private long modelStateSaveIntervalMillis = DEFAULT_MODEL_STATE_SAVE_INTERVAL_MILLIS;
    private long modelStateSaveJitterMillis = DEFAULT_MODEL_STATE_SAVE_JITTER_MILLIS;

    private static String defaultModelParametersPropertyGroupId(String modelPropertiesGroupIdPrefix) {
        return modelPropertiesGroupIdPrefix + DEFAULT_MODEL_PARAMETERS_PROPERTY_GROUP_ID_SUFFIX;
    }

    private static Properties getDefaultModelParametersProperties(Map<String, Properties> applicationProperties, String modelPropertiesGroupIdPrefix) {
        String defaultModelParametersPropertyGroupId = defaultModelParametersPropertyGroupId(modelPropertiesGroupIdPrefix);
        return applicationProperties.get(defaultModelParametersPropertyGroupId);
    }

    private static String extractModelKeyFromPropertyGroupId(String modelPropertiesGroupIdPrefix) {
        return StringUtils.removeStart(modelPropertiesGroupIdPrefix, "ModelParameters:");
    }


    public KeyRandomCutForestOperatorBuilder<IN, OUT> setInputMapper(RcfInputMapper<IN> inputMapper) {
        this.inputMapper = inputMapper;
        return this;
    }

    public KeyRandomCutForestOperatorBuilder<IN, OUT> setResultMapper(RcfResultMapper<IN, OUT> resultMapper) {
        this.resultMapper = resultMapper;
        return this;
    }

    public KeyRandomCutForestOperatorBuilder<IN, OUT> parseOperatorProperties(Properties operatorProperties) {
        this.modelStateSaveIntervalMillis = ApplicationConfigUtils.parseMandatoryLong(operatorProperties, MODEL_STATE_SAVE_INTERVAL_MS);
        this.modelStateSaveJitterMillis = ApplicationConfigUtils.parseMandatoryLong(operatorProperties, MODEL_STATE_SAVE_JITTER_MS);
        return this;
    }


    public KeyRandomCutForestOperatorBuilder<IN, OUT> setDefaultModelParameters(Properties defaultModelParamProperties) {
        RcfModelParams defaultModelParams = RcfModelParams.fromProperties(defaultModelParamProperties, DEFAULT_MODEL_PARAMETERS_PROPERTY_GROUP_ID_SUFFIX);
        this.modelsConfig = new RcfModelsConfig(defaultModelParams);
        return this;
    }

    public KeyRandomCutForestOperatorBuilder<IN, OUT> setDefaultModelParameters(Map<String, Properties> applicationProperties, String modelPropertiesGroupIdPrefix) {
        Properties defaultModelParamProperties = getDefaultModelParametersProperties(applicationProperties, modelPropertiesGroupIdPrefix);
        return setDefaultModelParameters(defaultModelParamProperties);
    }


    public KeyRandomCutForestOperatorBuilder<IN, OUT> setAllSpecificModelParametersProperties(Map<String, Properties> applicationProperties, String modelPropertiesGroupIdPrefix) {
        Preconditions.checkNotNull(this.modelsConfig, "Default model parameters must be set first");

        String defaultModelParametersPropertyGroupId = defaultModelParametersPropertyGroupId(modelPropertiesGroupIdPrefix);
        for (Map.Entry<String, Properties> entry : applicationProperties.entrySet()) {
            String propertyGroupId = entry.getKey();
            // Parse every PropertyGroup with a PropertyGroupId starting with the prefix, except the default model parameter group
            if (propertyGroupId.startsWith(modelPropertiesGroupIdPrefix) && !propertyGroupId.equalsIgnoreCase(defaultModelParametersPropertyGroupId)) {
                String modelKey = extractModelKeyFromPropertyGroupId(propertyGroupId);
                RcfModelParams modelParams = RcfModelParams.fromProperties(entry.getValue(), modelKey);
                this.modelsConfig.addModelParams(modelKey, modelParams);
            }
        }

        return this;
    }

    public KeyRandomCutForestOperator<IN, OUT> build() {
        Preconditions.checkNotNull(this.inputMapper, "Input mapper must be specified");
        Preconditions.checkNotNull(this.resultMapper, "Result mapper must be specified");
        Preconditions.checkNotNull(this.modelsConfig, "At least the default model parameters must be specified");

        LOG.info("Configuring KeyRandomCutForest operator:\n\tmodelStateSaveIntervalMillis: {}\n\tmodelStateSaveJitterMillis: {}\n\tDefault model parameters: {}",
                modelStateSaveIntervalMillis, modelStateSaveJitterMillis, modelsConfig.getDefaultModelParams());
        for (Map.Entry<String, RcfModelParams> entry : modelsConfig.getModelParamsMap().entrySet()) {
            LOG.info("\tModel key: {}, parameters: {}", entry.getKey(), entry.getValue());
        }

        return new KeyRandomCutForestOperator<>(
                inputMapper,
                resultMapper,
                modelsConfig,
                modelStateSaveIntervalMillis,
                modelStateSaveJitterMillis);
    }


}
