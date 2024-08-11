package software.amazon.flink.example.rcf.operator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Configuration (hyper-parameters) of a single RCF model.
 *
 * TODO add any other required parameter
 */
@Getter @AllArgsConstructor @Builder @ToString
public class RcfModelParams implements Serializable {
    private final int dimensions;
    private final int shingleSize;
    private final int numberOfTrees;
    private final int sampleSize;
    private final int outputAfter;
}
