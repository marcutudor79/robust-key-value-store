package keyValueStore;

import java.util.ArrayList;
import java.util.List;

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
	private int M = 49; // default to the max number of operations per process

	private Integer[] writeValue;
	private int v;
	private int operationsCompleted = 0;
	private int readenValue;

	private boolean isCrashed = false;
	private boolean isLaunched = false;
	private int sequenceNumber = 0;
	private int currentOpTimestamp;

	private boolean isWrite;
	private int ackCount;
	private List<ProcessMessage> readResponses = new ArrayList<>();

	// for logger
	private int processNumber;
	private String processName;
	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private long operationStartTime;


	public Process(){
		actorRefList = new ArrayList<>();
		writeValue = new Integer[M];

		//processes are called p + integer
		processNumber = Integer.parseInt(self().path().name().substring(1));
		//moved in the updateRef method where I compute N
		// for(int j = 0; j < M; j++){
		// 	writeValue[j] = j*N + processNumber;
		// }
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
		for(int j = 0; j < M; j++){
			writeValue[j] = j*N + processNumber;
		}
	}

    /* 6.REQ Upon receiving the CrashMessage, the process enters silent mode */
	public void onCrash(CrashMessage message){
		isCrashed = true;
		log.info(processName + ": " + "process crashed");
	}

	public void onLaunch(LaunchMessage message){
		if(isCrashed)	return;
		isLaunched = true;
		startOperation();
	}

	public void onReadRequest(ReadRequest message){
		if(isCrashed)	return;
		getSender().tell(new ProcessMessage(localValue,
						localTimestamp, message.getSequenceNumber()), self());
	}

	public void onReadResponse(ProcessMessage message) {
		if(isCrashed || !isLaunched)	return;
		if(message.getSequenceNumber() != sequenceNumber)	return;
		int sendValue;
		int sendTimestamp;
		readResponses.add(message);

		if(readResponses.size() == N/2 + 1){
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
			broadcastMessage(new WriteRequest(sendValue, sendTimestamp));

		}
	}

	public void onWriteRequest(WriteRequest message){
		if(isCrashed)	return;
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
		if (ack.getTimestamp() != currentOpTimestamp)	return;

		ackCount++;

        if (ackCount == (N / 2) + 1) {
			long timeSpent = System.nanoTime() - operationStartTime;
			if (isWrite) {
            	log.info(processName + ": " + "Put value: " + v + " operation duration: " + timeSpent +"ns");
			} else {
				log.info(processName + ": " + "Get return value: " + readenValue + " operation duration: " + timeSpent +"ns");
			}

            operationsCompleted++;
            ackCount = -1;
            startOperation();
        }
	}

	private void startOperation(){
		if(operationsCompleted == M*2){
			log.info(processName + ": " + "all operations completed");
			return;
		}
		ackCount = 0;

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
		readResponses.clear();
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
        log.info("Updated number of operations to " + M);
    }
}
