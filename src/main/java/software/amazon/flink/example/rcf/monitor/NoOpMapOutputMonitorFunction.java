package software.amazon.flink.example.rcf.monitor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import software.amazon.flink.example.rcf.model.OutputData;
import org.apache.flink.metrics.Counter;

/**
 * This class demonstrates how to implement a no-op map function
 * to maintain custom counters about scored records.
 */
public class NoOpMapOutputMonitorFunction extends RichMapFunction<OutputData, OutputData> {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(NoOpMapOutputMonitorFunction.class);


    // Counters
    private transient Counter processedRecordCount;
    private transient Counter scoredRecordCount;
    private transient Counter anomaliesCount;


    private final float anomalyThreshold;
    private final String metricGroupName;

    public NoOpMapOutputMonitorFunction(float anomalyThreshold, String metricGroupName) {
        this.anomalyThreshold = anomalyThreshold;
        this.metricGroupName = metricGroupName;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        processedRecordCount =  getRuntimeContext().getMetricGroup()
                .addGroup(metricGroupName)
                .counter("processedRecordCount");
        scoredRecordCount =  getRuntimeContext().getMetricGroup()
                .addGroup(metricGroupName)
                .counter("scoredRecordCount");
        anomaliesCount =  getRuntimeContext().getMetricGroup()
                .addGroup(metricGroupName)
                .counter("anomaliesCount");
    }

    @Override
    public OutputData map(OutputData record) throws Exception {
        processedRecordCount.inc();
        if( record.getScore() > 0.0) {
            scoredRecordCount.inc();
            if (record.getScore() > anomalyThreshold) {
                LOG.debug("Anomaly detected! {}", record);
                anomaliesCount.inc();
            }
        }

        // Pass-through
        return record;
    }
}
