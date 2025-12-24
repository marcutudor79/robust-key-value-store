package keyValueStore.msg;

import java.io.Serializable;

public class ProcessMessage implements Serializable{
    private static final long serialVersionUID = 1L;
    private final int value;
    private final int timestamp;
    private final int sequenceNumber;

    public ProcessMessage(int value, int timestamp, int sequenceNumber) {
        this.value = value;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
    }

    public int getValue() {
        return value;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

}
