package code;

import akka.actor.ActorRef;
import java.util.List;

public class ReferencesMessage {
    private final List<ActorRef> references;
    public ReferencesMessage(List<ActorRef> processes) {
        this.references = processes;
    }
    public List<ActorRef> getReferences() {
        return references;
    }
}
