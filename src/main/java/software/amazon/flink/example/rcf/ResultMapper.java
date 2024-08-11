package software.amazon.flink.example.rcf;

import software.amazon.flink.example.rcf.model.InputData;
import software.amazon.flink.example.rcf.model.OutputData;
import software.amazon.flink.example.rcf.operator.RcfResultMapper;

import java.util.Arrays;

public class ResultMapper implements RcfResultMapper<InputData, OutputData> {
    @Override
    public OutputData apply(InputData inputData, Double score) {
        return new OutputData(
                inputData.getTimestamp(),
                inputData.getKey(),
                Arrays.copyOf(inputData.getValues(), inputData.getValues().length),
                score);
    }
}
