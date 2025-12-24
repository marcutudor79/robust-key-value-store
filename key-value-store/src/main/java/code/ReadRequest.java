package code;

import java.io.Serializable;

public class ReadRequest implements Serializable{
    private static final long serialVersionUID = 1L;
    private final int sequenceNumber;

    public ReadRequest(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }  
}

