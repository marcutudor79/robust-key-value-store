package keyValueStore;

// AKKA imports
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.ActorRef;
import akka.actor.AbstractActor;

// Java imports
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
// Added imports for file writing
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import keyValueStore.msg.*;

public class Main {

    /* Logger */
    final static Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static class BenchmarkMonitor extends AbstractActor {
        private final int expectedMessages;
        private int receivedMessages = 0;
        private final long startTime;
        // Added fields for reporting
        private final int N;
        private final int f;
        private final int M;

        public BenchmarkMonitor(int expectedMessages, int N, int f, int M) {
            this.expectedMessages = expectedMessages;
            this.N = N;
            this.f = f;
            this.M = M;
            this.startTime = System.currentTimeMillis();
        }

        public static Props createActor(int expectedMessages, int N, int f, int M) {
            return Props.create(BenchmarkMonitor.class, () -> new BenchmarkMonitor(expectedMessages, N, f, M));
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .match(DoneMessage.class, msg -> {
                    receivedMessages++;
                    if (receivedMessages >= expectedMessages) {
                        long totalTime = System.currentTimeMillis() - startTime;
                        
                        // 1. Write result to file for the final table
                        try (FileWriter fw = new FileWriter("benchmark_results.csv", true);
                             PrintWriter out = new PrintWriter(fw)) {
                            out.printf("%-5d | %-5d | %-5d | %-5d ms%n", N, f, M, totalTime);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        // 2. Print completion to console
                        System.out.println("=========================================");
                        System.out.println("BENCHMARK FINISHED!");
                        System.out.println("TOTAL LATENCY: " + totalTime + " ms");
                        System.out.println("=========================================");
                        getContext().getSystem().terminate(); 
                    }
                })
                .build();
        }
    }

    public static void main(String[] args) {
        // 10.REQ & 10.1 REQ: dynamic arguments
        int numProcesses = Integer.parseInt(args[0]);
        int numCrashed = Integer.parseInt(args[1]);
        int numOperations = Integer.parseInt(args[2]);

        final ActorSystem system = ActorSystem.create("KeyValueStoreSystem");
        List<ActorRef> processRefs = new ArrayList<>();

        // 1.REQ Create N actors
        for (int i = 0; i < numProcesses; i++) {
            processRefs.add(system.actorOf(Process.createActor(), "p" + i));
        }

        // CREATE MONITOR (Updated to receive N, f, M for reporting)
        int expectedActive = numProcesses - numCrashed;
        ActorRef monitor = system.actorOf(BenchmarkMonitor.createActor(expectedActive, numProcesses, numCrashed, numOperations), "monitor");

        // 2.REQ Pass references (including monitor)
        ReferencesMessage referencesMessage = new ReferencesMessage(processRefs, monitor);
        OperationsMessage operationsMessage = new OperationsMessage(numOperations);
        
        for (ActorRef processRef : processRefs) {
            processRef.tell(referencesMessage, ActorRef.noSender());
            processRef.tell(operationsMessage, ActorRef.noSender());
        }

        // 5.REQ Select F random processes to crash
        List<ActorRef> toCrash = new ArrayList<>(processRefs);
        Collections.shuffle(toCrash);
        for (int i = 0; i < numCrashed; i++) {
            toCrash.get(i).tell(new CrashMessage(), ActorRef.noSender());
        }

        // 7.REQ Send LaunchMessage
        LaunchMessage launch = new LaunchMessage();
        for (int i = numCrashed; i < numProcesses; i++) {
            toCrash.get(i).tell(launch, ActorRef.noSender());
        }

        // WAIT FOR TERMINATION (Monitored by Actor)
        try {
            system.getWhenTerminated().toCompletableFuture().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}