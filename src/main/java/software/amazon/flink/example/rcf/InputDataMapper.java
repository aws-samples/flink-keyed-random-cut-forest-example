package software.amazon.flink.example.rcf;

import software.amazon.flink.example.rcf.model.InputData;
import software.amazon.flink.example.rcf.operator.RcfInputMapper;

import java.util.Arrays;

public class InputDataMapper implements RcfInputMapper<InputData> {
    @Override
    public float[] apply(InputData inputData) {
        // Return a copy of the values
        return Arrays.copyOf(inputData.getValues(), inputData.getValues().length);
    }
}
