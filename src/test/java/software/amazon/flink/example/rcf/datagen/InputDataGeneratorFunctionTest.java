package software.amazon.flink.example.rcf.datagen;

import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import software.amazon.flink.example.rcf.model.InputData;

@Disabled
public class InputDataGeneratorFunctionTest {

    /**
     * Not really a test.
     * This method uses the data generator function and displays generated values and anomalies
     */
    @Test
    public void displayGeneratedData() {
        InputDataGeneratorFunction generator = new InputDataGeneratorFunction();

        for(long seq = 0; seq < InputDataGeneratorFunction.PERIOD; seq++) {
            InputData data = generator.map(seq);

            System.out.println(generator.map(seq));
            for(int dim = 0; dim < InputDataGeneratorFunction.DIMENSIONS; dim++) {
                float value = data.getValues()[dim];
                if(value > InputDataGeneratorFunction.AMPLITUDE[dim]) {
                    System.out.println("Anomaly! in dim " + dim + ", value " + value);
                }
            }
        }
    }
}