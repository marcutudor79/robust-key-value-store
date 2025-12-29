package keyValueStore;

import java.io.Serializable;

public class Ack implements Serializable{

    private static final long serialVersionUID = 1L;
    private final int value;
    private final int timestamp;

    public Ack(int value, int timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public int getValue() {
        return value;
    }

    public int getTimestamp() {
        return timestamp;
    }
}
