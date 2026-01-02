package keyValueStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.actor.Props;

import keyValueStore.msg.ReferencesMessage;
import keyValueStore.msg.CrashMessage;
import keyValueStore.msg.LaunchMessage;
import keyValueStore.msg.OperationsMessage;
import keyValueStore.msg.ReadRequest;
import keyValueStore.msg.WriteRequest;
import keyValueStore.msg.ProcessMessage;

/* 3.REQ Use the name Process for the process class */
public class Process extends AbstractActor {
    private int localValue = 0;
    private int localTimestamp = 0;
    private int timestamp = 0;
    private List<ActorRef> actorRefList;
    private int N;
    private int M = 49;

    private Integer[] writeValue;
    private int v;
    private int operationsCompleted = 0;
    private int readenValue;

    private boolean isCrashed = false;
    private boolean isLaunched = false;
    private int sequenceNumber = 0;

    // Track CURRENT operation details to verify Acks
    private int currentOpTimestamp;
    private int currentOpValue; // ADDED: To verify Ack value

    private boolean isWrite;

    // REPLACED 'int ackCount' with Set to handle duplicates/robustness
    private Set<ActorRef> ackSenders = new HashSet<>();

    // REPLACED List-only logic with Set to handle duplicates
    private List<ProcessMessage> readResponses = new ArrayList<>();
    private Set<ActorRef> readResponseSenders = new HashSet<>();

    // for logger
    private int processNumber;
    private String processName;
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private long operationStartTime;

    public Process(){
        actorRefList = new ArrayList<>();
		// Note: writeValue initialization moved to updateOperations to ensure correct M
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

    public void updateReference(ReferencesMessage ref){
        actorRefList = ref.getReferences();
        N = actorRefList.size();
        // REMOVED: writeValue array filling here. M might be stale.
    }

    /* 6.REQ Upon receiving the CrashMessage, the process enters silent mode */
	public void onCrash(CrashMessage message){
		isCrashed = true;
		log.info(processName + ": " + "process crashed");
        getContext().stop(self()); // Terminate this actor
	}

    public void onLaunch(LaunchMessage message){
        if(isCrashed) return;
        isLaunched = true;
        startOperation();
    }

    public void onReadRequest(ReadRequest message){
        if(isCrashed) return;
        getSender().tell(new ProcessMessage(localValue,
                        localTimestamp, message.getSequenceNumber()), self());
    }

    public void onReadResponse(ProcessMessage message) {
        if(isCrashed || !isLaunched) return;
        if(message.getSequenceNumber() != sequenceNumber) return;

        // FIXED: Count unique senders only
        if (readResponseSenders.contains(getSender())) return;
        readResponseSenders.add(getSender());
        readResponses.add(message);

        // FIXED: Use logic "> N/2" (Majority)
        if(readResponseSenders.size() == (N / 2) + 1){
            int maxTs = Integer.MIN_VALUE;
            int maxVal = Integer.MIN_VALUE;

            for(ProcessMessage m: readResponses){
                if(m.getTimestamp() > maxTs){
                    maxTs = m.getTimestamp();
                    maxVal = m.getValue();
                } else if (m.getTimestamp() == maxTs && m.getValue() > maxVal) {
                    maxVal = m.getValue();
                }
            }

            int sendValue;
            int sendTimestamp;

            if(isWrite){
                timestamp = maxTs + 1;
                sendTimestamp = timestamp;
                sendValue = v;
            } else {
                sendTimestamp = maxTs;
                sendValue = maxVal;
                readenValue = sendValue;
            }

            currentOpTimestamp = sendTimestamp;
            currentOpValue = sendValue; // <--- SAVE value to verify Ack later

            broadcastMessage(new WriteRequest(sendValue, sendTimestamp));
        }
    }

    public void onWriteRequest(WriteRequest message){
        if(isCrashed) return;
        int timestampReq = message.getTimestamp();
        int valueReq = message.getValue();

        if((timestampReq > localTimestamp) ||
            (timestampReq == localTimestamp && valueReq > localValue))
        {
            localValue = valueReq;
            localTimestamp = timestampReq;
        }

        getSender().tell(new Ack(valueReq, timestampReq), self());
    }

    public void onAck(Ack ack){
        if (isCrashed || !isLaunched) return;

        // FIXED: Validate Timestamp AND Value (Safety Violation Fix)
        if (ack.getTimestamp() != currentOpTimestamp) return;
        if (ack.getValue() != currentOpValue) return;

        // FIXED: Count unique senders only (Robustness Fix)
        if (ackSenders.contains(getSender())) return;
        ackSenders.add(getSender());

        // FIXED: Use logic "> N/2" (Majority)
        if (ackSenders.size() == (N / 2) + 1) {
            long timeSpent = System.nanoTime() - operationStartTime;
            if (isWrite) {
                log.info(processName + ": " + "Put value: " + v + " operation duration: " + timeSpent +"ns");
            } else {
                log.info(processName + ": " + "Get return value: " + readenValue + " operation duration: " + timeSpent +"ns");
            }

            operationsCompleted++;
            startOperation();
        }
    }

	private void startOperation(){
		if(operationsCompleted == M*2){
			log.info(processName + ": " + "all operations completed");
            getContext().stop(self()); // Terminate this actor
			return;
		}
		ackSenders.clear();
        readResponses.clear();
        readResponseSenders.clear();

        /*
            8.REQ Upon receiving the LaunchMessage, the process starts executing put and get operations
            - M put operations with k = 1 and v = i, N+i, 2N+i, ... MN+i
            - M get operations for k = 1
        */
        operationStartTime = System.nanoTime();
        if(operationsCompleted < M){
            isWrite = true;
            v = writeValue[operationsCompleted];
            log.info(processName + ": " + "Invoke write");
        }
        else {
            isWrite = false;
            log.info(processName + ": " + "Invoke read");
        }

        sequenceNumber++;
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