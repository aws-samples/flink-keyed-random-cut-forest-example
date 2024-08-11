package software.amazon.flink.example.rcf.model;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class OutputData {
    private long timestamp;
    private String key;
    private float[] values;
    private double score;
}
