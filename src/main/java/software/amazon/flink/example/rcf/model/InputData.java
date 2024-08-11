package software.amazon.flink.example.rcf.model;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class InputData {

    private long timestamp;
    private String key;
    private float[] values;
}


