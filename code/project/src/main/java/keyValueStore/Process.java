
package keyValueStore;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import keyValueStore.msg.CrashMessage;
import keyValueStore.msg.LaunchMessage;
import keyValueStore.msg.OperationsMessage;
import keyValueStore.msg.ProcessMessage;
import keyValueStore.msg.ReadRequest;
import keyValueStore.msg.ReferencesMessage;
import keyValueStore.msg.WriteRequest;
import keyValueStore.msg.DoneMessage;
/* 3.REQ Use the name Process for the process class */
public class Process extends AbstractActor {
    // line 1: locally stored value, initially 0
    private int localValue = 0;
    // line 2: locally stored timestamp, initially 0
    private int localTimestamp = 0;
    // line 3: timestamp
    private int timestamp = 0;
    // line 4: sequence number (number of issued read requests)
    private int sequenceNumber = 0;
    private List<ActorRef> actorRefList;
    private int N;
    private int M = 49;
    private Integer[] writeValue;
    private int v;
    private int operationsCompleted = 0;
    private int readenValue;
    private boolean isCrashed = false;
    private boolean isLaunched = false;
    // Track CURRENT operation details to verify Acks
    private int currentOpTimestamp;
    private int currentOpValue; // ADDED: To verify Ack value
    private boolean isWrite;
    // REPLACED 'int ackCount' with Set to handle duplicates/robustness
    private Set<ActorRef> ackSenders = new HashSet<>();
    // REPLACED List-only logic with Set to handle duplicates
    // lines 9 and 18: wait until received from a majority
    private List<ProcessMessage> readResponses = new ArrayList<>();
    private Set<ActorRef> readResponseSenders = new HashSet<>();
    // for logger
    private int processNumber;
    private String processName;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private long operationStartTime;
    private ActorRef monitor;
    public Process(){
        actorRefList = new ArrayList<>();
        // Note: writeValue initialization moved to updateOperations to ensure correct M
        /* the processes are named "p" + number of the process */
        processNumber = Integer.parseInt(self().path().name().substring(1));
        processName = "p" + processNumber;
    }
    public static Props createActor() {
        return Props.create(Process.class, () -> {
            return new Process();
        });
    }
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(ReferencesMessage.class, this::updateReference)
            .match(CrashMessage.class, this::onCrash)
            .match(LaunchMessage.class, this::onLaunch)
            /* 4.REQ Process class creates methods for executing put and get operations */
            .match(ReadRequest.class, this::onReadRequest)
            .match(ProcessMessage.class, this::onReadResponse)
            /* 4.REQ Process class creates methods for executing put and get operations */
            .match(WriteRequest.class, this::onWriteRequest)
            .match(Ack.class, this::onAck)
            .match(OperationsMessage.class, this::updateOperations)
            .build();
    }
    // Receives the reference list of all other processes to establish connectivity
    public void updateReference(ReferencesMessage ref){
        actorRefList = ref.getReferences();
        monitor = ref.getMonitor();
        N = actorRefList.size();
        // REMOVED: writeValue array filling here. M might be stale.
    }
    /* 6.REQ Upon receiving the CrashMessage, the process enters silent mode */
    public void onCrash(CrashMessage message){
        isCrashed = true;
        log.info(processName + ": " + "process crashed");
        // getContext().stop(self()); // Terminate this actor  <-- REMOVED to honor "silent mode"
    }
    public void onLaunch(LaunchMessage message){
        if(isCrashed) return;
        isLaunched = true;
        /*8.REQ Upon receiving the LaunchMessage, the process starts executing put and get operations:
        - M put operations with k = 1 and v = i, N+i, 2N+i, ... MN+i
        - M get operations for k = 1 */
        startOperation();
    }
    // line 28: Upon received [?,r'] from p_j
    /* responds with the current local value and timestamp */
    public void onReadRequest(ReadRequest message){
        if(isCrashed) return;
        // line 29: send [localValue, localTimestamp, r'] to p_j
        getSender().tell(new ProcessMessage(localValue,
                localTimestamp, message.getSequenceNumber()), self());
    }
    // lines 9 to 12 and 18 to 20
    /* Collects local values and timestamps from peers.
    Once a majority is reached, it determines the most recent value
    (i.e., the highest timestamp) and broadcasts the write request. */
    public void onReadResponse(ProcessMessage message) {
        if(isCrashed || !isLaunched) return;
        if(message.getSequenceNumber() != sequenceNumber) return;
        // FIXED: Count unique senders only
        if (readResponseSenders.contains(getSender())) return;
        readResponseSenders.add(getSender());
        // lines 9 and 18 (collect responses)
        readResponses.add(message);
        // majority reached lines 9 and 18
        if(readResponseSenders.size() >= (N / 2) + 1){
            int maxTs = Integer.MIN_VALUE;
            int maxVal = Integer.MIN_VALUE;
            for(ProcessMessage m: readResponses){
                if(m.getTimestamp() > maxTs){
                    maxTs = m.getTimestamp(); // lines 10 and 19
                    maxVal = m.getValue(); // line 19
                } else if (m.getTimestamp() == maxTs && m.getValue() > maxVal) {
                    maxVal = m.getValue(); // line 19
                }
            }
            int sendValue;
            int sendTimestamp;
            if(isWrite){
                timestamp = maxTs + 1; // line 11
                sendTimestamp = timestamp; // line 12
                sendValue = v; // line 12
            } else {
                sendTimestamp = maxTs; // line 20
                sendValue = maxVal; // line 20
                readenValue = sendValue;
            }
            currentOpTimestamp = sendTimestamp;
            currentOpValue = sendValue; // SAVE value to verify Ack later
            // lines 12 and 20: send [v, t] to all
            broadcastMessage(new WriteRequest(sendValue, sendTimestamp));
        }
    }
    // line 23: Upon received [v', t'] from p
    /* updates the local storage if the incoming request is newer.
    It then sends back an ACK. */
    public void onWriteRequest(WriteRequest message){
        if(isCrashed) return;
        int timestampReq = message.getTimestamp();
        int valueReq = message.getValue();
        // line 24: if t' > localTS or (t' = localTS and v' > localValue)
        if((timestampReq > localTimestamp) ||
           (timestampReq == localTimestamp && valueReq > localValue))
        {
            localValue = valueReq; // line 25
            localTimestamp = timestampReq; // line 26
        }
        // line 27: send [ack, v', t'] to p
        getSender().tell(new Ack(valueReq, timestampReq), self());
    }
    /* Counts ACKs to verify that the value has been safely stored on a majority of nodes. */
    public void onAck(Ack ack){
        if (isCrashed || !isLaunched) return;
        // FIXED: Validate Timestamp AND Value (Safety Violation Fix)
        if (ack.getTimestamp() != currentOpTimestamp) return;
        if (ack.getValue() != currentOpValue) return;
        // FIXED: Count unique senders only (Robustness Fix)
        if (ackSenders.contains(getSender())) return;
        ackSenders.add(getSender());
        // lines 13 and 21: wait until received [ack, v, t] from a majority
        if (ackSenders.size() >= (N / 2) + 1) {
            // 11.REQ: Measure latency (End Timer & Calculation)
            long timeSpent = System.nanoTime() - operationStartTime;
            if (isWrite) {
                log.info(processName + ": " + "Put value: " + v + " operation duration: " + timeSpent +"ns end_ts=" + System.nanoTime() + " seq=" + sequenceNumber);
            } else {
                log.info(processName + ": " + "Get return value: " + readenValue + " operation duration: " + timeSpent +"ns end_ts=" + System.nanoTime() + " seq=" + sequenceNumber);
            }
            operationsCompleted++;
            // 9.REQ: Every process performs at most one operation at a time (next op starts only after current completes)
            startOperation();
        }
    }
    // it starts the next operation
    private void startOperation(){
        /*8.REQ Upon receiving the LaunchMessage, the process starts executing put and get operations:
        - M put operations with k = 1 and v = i, N+i, 2N+i, ... MN+i
        - M get operations for k = 1 */
        if(operationsCompleted == M*2){
            log.info(processName + ": " + "all operations completed");
            // Notify monitor before terminating to allow scenario completion and CSV writing
            if (monitor != null) {
                monitor.tell(new DoneMessage(), self());
            }
            // getContext().stop(self()); // Terminate this actor
            return;
        }
        ackSenders.clear();
        readResponses.clear();
        readResponseSenders.clear();
        // 11.REQ: Measure latency (Start Timer)
        operationStartTime = System.nanoTime();
        /*
        8.REQ Upon receiving the LaunchMessage, the process starts executing put and get operations
        - M put operations with k = 1 and v = i, N+i, 2N+i, ... MN+i
        - M get operations for k = 1
        */
        if(operationsCompleted < M){
            isWrite = true;
            v = writeValue[operationsCompleted];
            log.info(processName + ": " + "Invoke write start_ts=" + operationStartTime + " seq=" + sequenceNumber);
        }
        else {
            isWrite = false;
            log.info(processName + ": " + "Invoke read start_ts=" + operationStartTime + " seq=" + sequenceNumber);
        }
        sequenceNumber++; // lines 7 and 16
        // lines 8 and 17: send [?, r] to all
        ReadRequest req = new ReadRequest(sequenceNumber);
        broadcastMessage(req);
    }
    private void broadcastMessage(Object msg){
        for (ActorRef actor : actorRefList) {
            actor.tell(msg, self());
        }
    }
    public void updateOperations(OperationsMessage msg) {
        this.M = msg.getNumOperations();
        /*
        8.REQ Upon receiving the LaunchMessage, the process starts executing put and get operations
        - M put operations with k = 1 and v = i, N+i, 2N+i, ... MN+i
        - M get operations for k = 1
        */
        // FIXED: Initialize array here with the correct M
        writeValue = new Integer[M];
        for(int j = 0; j < M; j++){
            // Formula from project requirement: v = i + k*N
            // processNumber is 'i', j is 'k'
            writeValue[j] = j * N + processNumber;
        }
        log.info("Updated number of operations to " + M);
    }
}
