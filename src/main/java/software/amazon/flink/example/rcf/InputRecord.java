package software.amazon.flink.example.rcf;

public class InputRecord {

    public String key;
    public float[] values;

    public InputRecord(String key, float[] values) {
        this.key = key;
        this.values = values;
    }

    public InputRecord() {
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public float[] getValues() {
        return values;
    }

    public void setValues(float[] values) {
        this.values = values;
    }
}
