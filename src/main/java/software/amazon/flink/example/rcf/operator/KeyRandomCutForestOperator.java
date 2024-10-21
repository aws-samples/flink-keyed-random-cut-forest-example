package software.amazon.flink.example.rcf.operator;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.amazon.randomcutforest.state.RandomCutForestState;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Keyed, stateful RCF operator.
 * <p>
 * TODO explain how it works
 *
 * TODO RandonCutForest mapper can be configured further. See https://github.com/aws/random-cut-forest-by-aws/blob/35f4cf6cfbe9e2ac0a9b35b88034800f36bec181/Java/core/src/main/java/com/amazon/randomcutforest/state/RandomCutForestMapper.java#L57
 */
public class KeyRandomCutForestOperator<IN, OUT> extends KeyedProcessFunction<String, IN, OUT> {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(KeyRandomCutForestOperator.class);

    private static final RandomCutForestMapper rcfMapper = new RandomCutForestMapper();

    static {
        // The Executor Context is not serialized by default - this is required
        rcfMapper.setSaveExecutorContextEnabled(true);
        // Enable compression to reduce state size - this is optional
        rcfMapper.setCompressionEnabled(true);
    }

    // Cached, non-serializable RCF models, by key
    private transient ConcurrentMap<String, RandomCutForest> rcfModel;

    // Flags to mark initialized timers, per key
    private transient ConcurrentMap<String, Boolean> modelStateSaveTimerSet;


    // (Keyed) State containing the RCF Model state (serializable) for the key
    private transient ValueState<RandomCutForestState> rcfState;
    private static final ValueStateDescriptor<RandomCutForestState> RCF_STATE_DESCRIPTOR =
            new ValueStateDescriptor<>("rcf-state", RandomCutForestState.class);

    // Maps input data to float[]
    private final RcfInputMapper<IN> inputMapper;

    // Maps score + input data to output
    private final RcfResultMapper<IN, OUT> resultMapper;

    private final RcfModelsConfig modelsConfig;
    private final long modelStateSaveIntervalMillis;
    private final long modelStateSaveTimerJitterMillis;


    public KeyRandomCutForestOperator(
            RcfInputMapper<IN> inputMapper,
            RcfResultMapper<IN, OUT> resultMapper,
            RcfModelsConfig modelsConfig,
            long modelStateSaveIntervalMillis,
            long modelStateSaveTimerJitterMillis) {
        Preconditions.checkNotNull(modelsConfig, "Models config must be provided");
        Preconditions.checkArgument(modelStateSaveTimerJitterMillis >= 0, "Model save state jitter must be >= 0");
        Preconditions.checkArgument(modelStateSaveIntervalMillis > modelStateSaveTimerJitterMillis, "Model save state interval must be larger than the jitter");

        this.inputMapper = inputMapper;
        this.resultMapper = resultMapper;
        this.modelsConfig = modelsConfig;
        this.modelStateSaveIntervalMillis = modelStateSaveIntervalMillis;
        this.modelStateSaveTimerJitterMillis = modelStateSaveTimerJitterMillis;

        LOG.info("Models configuration:\n{}", modelsConfig);
    }


    /**
     * This method is called once, when the operator is initialized.
     * It is used to initialize the Flink state handles and any transient field.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialize an empty map of cached RCF models. The actual models will be initialized lazily
        rcfModel = new ConcurrentHashMap<>();

        // Initialize an empty map for timers
        modelStateSaveTimerSet = new ConcurrentHashMap<>();

        // Initialize the RCF state
        rcfState = getRuntimeContext().getState(RCF_STATE_DESCRIPTOR);
    }

    /**
     * Process an input record.
     */
    @Override
    public void processElement(IN inputData, KeyedProcessFunction<String, IN, OUT>.Context ctx, Collector<OUT> out) throws Exception {
        String modelKey = ctx.getCurrentKey();

        // Get the model for this modelKey. The model is lazily initialized, either restoring from Flink state or initialized (empty)
        // from the model parameters in the configuration.
        RandomCutForest model = getRcfModel(modelKey);

        // Convert the input into float[]
        float[] rcfInput = inputMapper.apply(inputData);
        Preconditions.checkArgument(
                rcfInput.length * model.getShingleSize() == model.getDimensions(),
                "Input data nr of dimensions does not match model dimension / shingles");

        // Score and update the model
        double score = model.getAnomalyScore(rcfInput);
        model.update(rcfInput);
        LOG.trace("Scored {} for input {} on modelKey {}", score, rcfInput, modelKey);

        // Emit output
        out.collect(resultMapper.apply(inputData, score));

        // Set a timer to extract the RCFState to RCFModel, for the current modelKey only, if not already set
        if (!modelStateSaveTimerSet.getOrDefault(modelKey, false)) {
            setNewTimerForKey(modelKey, ctx.timerService());
        }
    }

    /**
     * Every time a timer is triggered, the RCF state is extracted from the cached RCF model and put in Flink state,
     * and set a new timer for the same modelKey.
     * Timers are per-key, so only the model for a specific key is saved.
     */
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, IN, OUT>.OnTimerContext ctx, Collector<OUT> out) throws Exception {
        String modelKey = ctx.getCurrentKey();
        LOG.trace("Executing model state save timer for modelKey: {}", modelKey);

        // Extract RCFState from RCFModel (for the current modelKey only)
        saveRcfState(modelKey);

        // Set the next timer for this modelKey
        setNewTimerForKey(modelKey, ctx.timerService());
    }

    /**
     * Get the model, lazily initializing it
     */
    private RandomCutForest getRcfModel(String modelKey)  {
        // If there is not cached RCF model for this modelKey, initialize it
        return rcfModel.computeIfAbsent(modelKey, this::restoreOrInitModel);
    }


    /**
     * Restore the RCF model from state, if a serializable state is available. Otherwise, initialize it from scratch
     */
    private RandomCutForest restoreOrInitModel(String modelKey)  {
        try {
            if (rcfState.value() != null) {
                // If there is an RCF state in state, restore the model from state
                LOG.info("Restoring the RCF model for modelKey '{}' from state", modelKey);
                RandomCutForestState modelState = rcfState.value();
                return rcfMapper.toModel(modelState);
            } else {
                // Otherwise, initialize the (untrained) model based on configuration
                RcfModelParams modelParams = modelsConfig.getModelParams(modelKey);
                LOG.info("Initialising the RCF model for modelKey '{}' from params: {}", modelKey, modelParams);
                return initialiseModel(modelParams);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to restore or initialize RCF model for modelKey: " + modelKey, ex);
        }
    }

    /**
     * Initialize a new RCF model from model parameters
     */
    private RandomCutForest initialiseModel(RcfModelParams modelParams) {
        return RandomCutForest.builder()
                .dimensions(modelParams.getDimensions() * modelParams.getShingleSize())
                .shingleSize(modelParams.getShingleSize())
                .numberOfTrees(modelParams.getNumberOfTrees())
                .sampleSize(modelParams.getSampleSize())
                .outputAfter(modelParams.getOutputAfter())
                // TODO use any additional supported parameter
                .build();
    }


    /**
     * Gets the cached, in-memory RCF Model for the specified modelKey (if any), extract the RCFState, and save it in Flink state.
     */
    private void saveRcfState(String modelKey) throws Exception {

        // If the model for this key is initialized, save its serialized form in Flink state
        RandomCutForest model = rcfModel.getOrDefault(modelKey, null);
        if (model != null) {
            LOG.info("Saving state RCF model for modelKey: '{}' in Flink state", modelKey); // FIXME change log level to DEBUG
            // Extract the model serializable state
            RandomCutForestState modelState = rcfMapper.toState(model);
            // Store the model state in Flink state
            rcfState.update(modelState);
        } else {
            // If the timer fires while the model is not initialized yet, do nothing. This may happen when restoring
            // from checkpoint: timers are saved with the checkpoint/savepoint, and all expired timers are triggered
            // immediately when the state is restored. We are just logging for testing here
            LOG.info("Save RCF state timer fired but the model is not initialized, modelKey: '{}'" +
                    " (this can happen when the application is restored from snapshot)", modelKey);
        }
    }

    /**
     * Set the timer for the modelKey
     */
    private void setNewTimerForKey(String modelKey, TimerService timerService) {
        // Calculate the new timer, adding a random jitter, if configured
        long currentTime = timerService.currentProcessingTime();
        long jitter = (modelStateSaveTimerJitterMillis > 0)
                ? (long) (RandomUtils.nextDouble(0, 1) * modelStateSaveTimerJitterMillis)
                : 0;
        long nextTimer = currentTime + modelStateSaveIntervalMillis + jitter;

        // Set the next timer
        LOG.debug("Setting model state save timer for modelKey: '{}', at {} in {} millis", modelKey, Instant.ofEpochMilli(nextTimer), nextTimer - currentTime);
        timerService.registerProcessingTimeTimer(nextTimer);

        // Ensure the flag of the timer is set for this modelKey
        modelStateSaveTimerSet.put(modelKey, true);
    }

}
