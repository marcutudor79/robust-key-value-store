package keyValueStore.msg;

import akka.actor.ActorRef;
import java.util.List;
import java.io.Serializable;

public class ReferencesMessage implements Serializable {
    private final List<ActorRef> references;
    public ReferencesMessage(List<ActorRef> processes) {
        this.references = processes;
    }
    public List<ActorRef> getReferences() {
        return references;
    }
}
