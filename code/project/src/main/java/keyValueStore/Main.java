package keyValueStore;

// AKKA imports
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.ActorRef;

// Java imports
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;

// KeyValueStore imports
import keyValueStore.msg.ReferencesMessage;

public class Main {

    /* Logger */
    final static Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {

        /* Validate numProcesses arg */
        int numProcesses = -1;
        try {
            numProcesses = Integer.parseInt(args[0]);
            if (numProcesses < 0) {
                throw new IllegalArgumentException("First argument (no of processes) must be a positive integer.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("First argument must be a valid integer.", e);
        }

        LOGGER.config("The number of processes: " + numProcesses);

        /* Create the Actor System in AKKA
            - create numProcesses actors
            - pass a reference to each actor to every other actor
        */
        final ActorSystem system = ActorSystem.create("KeyValueStoreSystem");
        List<akka.actor.ActorRef> processRefs = new ArrayList<>();


        for (int i = 0; i < numProcesses; i++) {
            Props process = Process.createActor();
            try {
                ActorRef processRef = system.actorOf(process, "Process" + i);
                processRefs.add(processRef);
                LOGGER.info("Created actor Process" + i);
            } catch (Exception e) {
                LOGGER.severe("Failed to create actor Process" + i);
            }
        }

        /* Send references to the processes */
        ReferencesMessage referencesMessage = new ReferencesMessage(processRefs);
        for (ActorRef processRef : processRefs) {
            processRef.tell(referencesMessage, ActorRef.noSender());
        }

        
    }
}
