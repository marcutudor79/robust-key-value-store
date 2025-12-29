package keyValueStore;

// AKKA imports
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.ActorRef;

// Java imports
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import keyValueStore.msg.CrashMessage;
import keyValueStore.msg.LaunchMessage;
import keyValueStore.msg.OperationsMessage;
import java.util.Collections;

// KeyValueStore imports
import keyValueStore.msg.ReferencesMessage;

public class Main {

    /* Logger */
    final static Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {

        /* Validate arguments */
        int numProcesses  = -1;
        int numCrashed    = -1;
        int numOperations = -1;
        try {
            numProcesses  = Integer.parseInt(args[0]);
            numCrashed    = Integer.parseInt(args[1]);
            numOperations = Integer.parseInt(args[2]);
            if (numProcesses < 0 || numOperations < 0 || numCrashed < 0) {
                throw new IllegalArgumentException("Arguments must be non-negative integers.");
            }
            if (numCrashed >= numProcesses) {
                throw new IllegalArgumentException("Second argument must be less than the number of processes.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Arguments must be valid integers.");
        }

        LOGGER.config("The number of processes: " + numProcesses);
        LOGGER.config("The number of crashed processes: " + numCrashed);
        LOGGER.config("The number of operations per process: " + numOperations);

        /* Create the Actor System in AKKA
            - create numProcesses actors
            - pass a reference to each actor to every other actor
        */
        final ActorSystem system = ActorSystem.create("KeyValueStoreSystem");
        List<akka.actor.ActorRef> processRefs = new ArrayList<>();

        /* 1.REQ Main class creates N actors */
        for (int i = 0; i < numProcesses; i++) {
            Props process = Process.createActor();
            try {
                ActorRef processRef = system.actorOf(process, "p" + i);
                processRefs.add(processRef);
                LOGGER.info("Created actor Process" + i);
            } catch (Exception e) {
                LOGGER.severe("Failed to create actor Process" + i);
            }
        }

        /* 2.REQ Main class passes references of all actors to each actor */
        ReferencesMessage referencesMessage = new ReferencesMessage(processRefs);
        OperationsMessage operationsMessage = new OperationsMessage(numOperations);
        for (ActorRef processRef : processRefs) {
            processRef.tell(referencesMessage, ActorRef.noSender());
            processRef.tell(operationsMessage, ActorRef.noSender());
        }

        /* 5.REQ Main class selects F random processes then sends a special CrashMessage to each of them */
		List<ActorRef> toCrash = new ArrayList<>(processRefs);
		Collections.shuffle(toCrash);

		for (int i = 0; i < numCrashed; i++)
			toCrash.get(i).tell(new CrashMessage(), ActorRef.noSender());

		/* 7.REQ Main class sends a LaunchMessage to all non-crashed processes */
		LaunchMessage launch = new LaunchMessage();

		for (int i = numCrashed; i < numProcesses; i++)
			toCrash.get(i).tell(launch, ActorRef.noSender());
    }
}
