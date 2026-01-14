package keyValueStore.msg;
import akka.actor.ActorRef;
import java.util.List;
import java.io.Serializable;

public class ReferencesMessage implements Serializable {
    private final List<ActorRef> references;
    private final ActorRef monitor; // Added field for automatic termination

    public ReferencesMessage(List<ActorRef> processes, ActorRef monitor) {
        this.references = processes;
        this.monitor = monitor;
    }
    public List<ActorRef> getReferences() {
        return references;
    }
    public ActorRef getMonitor() {
        return monitor;
    }
}