package keyValueStore.msg;

import java.io.Serializable;

public class OperationsMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int numOperations;

    public OperationsMessage(int numOperations) {
        this.numOperations = numOperations;
    }

    public int getNumOperations() {
        return numOperations;
    }
}
