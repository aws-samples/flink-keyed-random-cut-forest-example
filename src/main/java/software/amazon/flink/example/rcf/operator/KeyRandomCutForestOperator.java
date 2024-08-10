package software.amazon.flink.example.rcf.operator;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyRandomCutForestOperator<IN, OUT> extends KeyedProcessFunction<String, IN, OUT> {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(KeyRandomCutForestOperator.class);

    // Cached, non-serializable RCF models, by key
    private transient Map<String, String> rcfModel; // TODO will be  Map<String, RandomCutForest>

    private transient ConcurrentHashMap<String, Boolean> modelStateSaveTimerSet;


    // (Keyed) State containing the RCF Model state (serializable) for the key
    private transient ValueState<String> rcfState; // TODO will be ValueState<RandomCutForestState>
    private static final ValueStateDescriptor<String> RCF_STATE_DESCRIPTOR = new ValueStateDescriptor<>("rcf-state", String.class); // TODO will be ValueStateDescriptor<RandomCutForestState>


    private final RcfInputMapper<IN> inputMapper;
    private final RcfResultMapper<IN, OUT> resultMapper;
    private final long modelStateSaveIntervalMillis;
    private final long modelStateSaveTimerJitterMillis;

    public KeyRandomCutForestOperator(RcfInputMapper<IN> inputMapper, RcfResultMapper<IN, OUT> resultMapper, long modelStateSaveIntervalMillis, long modelStateSaveTimerJitterMillis) {
        Preconditions.checkArgument(modelStateSaveTimerJitterMillis >= 0, "Model save state jitter must be >= 0");
        Preconditions.checkArgument(modelStateSaveIntervalMillis > modelStateSaveTimerJitterMillis, "Model save state interval must be larger than the jitter");

        this.inputMapper = inputMapper;
        this.resultMapper = resultMapper;
        this.modelStateSaveIntervalMillis = modelStateSaveIntervalMillis;
        this.modelStateSaveTimerJitterMillis = modelStateSaveTimerJitterMillis;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Initialize an empty map of cached RCF models. The actual models will be initialized lazily
        rcfModel = new HashMap<>();

        // Initialize an empty map for timers
        modelStateSaveTimerSet = new ConcurrentHashMap<>();

        // Initialize the RCF state
        rcfState = getRuntimeContext().getState(RCF_STATE_DESCRIPTOR);
    }

    @Override
    public void processElement(IN inputData, KeyedProcessFunction<String, IN, OUT>.Context ctx, Collector<OUT> out) throws Exception {
        String key = ctx.getCurrentKey();

        String model = getModel(key); // TODO will be RCFModel

        // Lazily initialise the RCF model for the given key
        if (!rcfModel.containsKey(key)) {
            rcfModel.put(key, initialiseModel(key));
        }

        // Convert the input into float[]
        float[] rcfInput = inputMapper.apply(inputData);
        // TODO check the input array has the same size of the number of dimensions in the model hyper-parameters for this key

        // Score
        double score = score(rcfInput, model);

        // Update the model (train), adding the input
        updateModel(rcfInput, model);

        // Emit output
        out.collect(resultMapper.apply(inputData, score));

        // Set a timer to extract the RCFState to RCFModel, for the current key only, if not already set
        conditionallySetModelSaveTimerForKey(key, ctx);
    }

    /**
     * Set the timer for the key, if not already set
     */
    private void conditionallySetModelSaveTimerForKey(String key, KeyedProcessFunction<String, IN, OUT>.Context ctx) {
        if (!modelStateSaveTimerSet.getOrDefault(key, false)) {
            setModelSaveTimerForKey(key, ctx.timerService());
        }
    }

    /**
     * Set the timer for the key
     */
    private void setModelSaveTimerForKey(String key, TimerService timerService) {
        // Calculate the new state save timer, introducing a random jitter
        long currentTime = timerService.currentProcessingTime();
        long jitter = (modelStateSaveTimerJitterMillis > 0)
                ? (long)(RandomUtils.nextDouble(0, 2) * modelStateSaveTimerJitterMillis) - modelStateSaveTimerJitterMillis
                : 0;
        long nextTimer = currentTime + modelStateSaveIntervalMillis + jitter;

        // Set the next timer
        LOG.info("Setting model state save timer for key: {}, at {} in {} millis", key, Instant.ofEpochMilli(nextTimer), nextTimer - currentTime);
        timerService.registerProcessingTimeTimer(nextTimer);

        // Ensure the flag of the timer is set for this key
        modelStateSaveTimerSet.put(key, true);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, IN, OUT>.OnTimerContext ctx, Collector<OUT> out) throws Exception {
        String key = ctx.getCurrentKey();
        LOG.info("Executing model state save timer for key: {}", key);

        // Extract RCFState from RCFModel (for the current key only)
        saveRcfState(key);

        // Set the next timer for this key
        setModelSaveTimerForKey(key, ctx.timerService());
    }


    /**
     * Gets the in-memory RCFModel for the specified key (if any), extract the RCFState and save it in state.
     */
    private void saveRcfState(String key) throws Exception {
        if (rcfModel.containsKey(key)) {
            LOG.info("Saving state RCFModel(key: {}) in state", key);
            String model = rcfModel.get(key); // TODO will be RCFModel
            String modelState = extractModelState(model); // TODO will be RCFState

            // Store RCFState in state
            rcfState.update(modelState);
        } else {
            LOG.info("No RCFModel for key {} to save", key);
        }
    }


    /**
     * Lazily initialize the RCF Model
     * TODO will return RandomCutForest
     */
    private String getModel(String key) throws Exception {
        // If there is not cached RCF model for this key, initialize it
        if (!rcfModel.containsKey(key)) {
            if (rcfState.value() != null) {
                // If there is an RCF state in state, restore the model from state
                String model = restoreModel(rcfState.value());
                rcfModel.put(key, model);
            } else {
                // Otherwise, initialize the (untrained) model from configuration
                rcfModel.put(key, initialiseModel(key));
            }
        }
        return rcfModel.get(key);
    }

    // TODO will return RandomCutForest
    private String initialiseModel(String key) {
        LOG.info("Initialising RCF model, key: {}", key);
        // TODO implement RCF initialization based on hyper-params passed to the class via constructor
        return "RCFModel(key: " + key + ")";
    }

    /**
     * Restore the RCF model from the model state
     * TODO will receive RCFstate and return RandomCutForest
     */
    private String restoreModel(String modelState) {
        LOG.info("Restoring RCF model from state: {}", modelState);
        return "RCFModel from " + modelState;
    }

    // FIXME the model will be RCFModel
    private double score(float[] input, String model) {
        LOG.debug("Scoring input {} against model {}", input, model);
        return 0.0;
    }

    // FIXME the model will be RCFModel
    private void updateModel(float[] input, String model) {
        LOG.debug("Updating model {} with input {}", model, input);
    }

    // FIXME will receive RCFModel and return RCFState
    private String extractModelState(String model) {
        LOG.info("Extracting state from model {}", model);
        return "RCFState of " + model;
    }
}
