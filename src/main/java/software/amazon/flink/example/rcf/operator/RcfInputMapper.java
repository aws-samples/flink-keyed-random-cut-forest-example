package software.amazon.flink.example.rcf.operator;

import org.apache.flink.api.common.functions.Function;

/**
 * Maps the model input to a float array
 */
@FunctionalInterface
public interface RcfInputMapper<IN> extends Function, java.util.function.Function<IN, float[]> {
}
