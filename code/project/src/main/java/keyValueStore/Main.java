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
import java.util.Collections;

// KeyValueStore imports
import keyValueStore.msg.ReferencesMessage;

public class Main {

    /* Logger */
    final static Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {

        /* Validate numProcesses arg */
        int numProcesses = -1;
        int numCrashed   = -1;
        try {
            numProcesses = Integer.parseInt(args[0]);
            numCrashed   = Integer.parseInt(args[1]);
            if (numProcesses < 0) {
                throw new IllegalArgumentException("First argument (no of processes) must be a positive integer.");
            }
            if (numCrashed < 0 || numCrashed >= numProcesses) {
                throw new IllegalArgumentException("Second argument (no of crashed processes) must be a non-negative integer less than the number of processes.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Arguments must be valid integers.", e);
        }

        LOGGER.config("The number of processes: " + numProcesses);
        LOGGER.config("The number of crashed processes: " + numCrashed);

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
        for (ActorRef processRef : processRefs) {
            processRef.tell(referencesMessage, ActorRef.noSender());
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
