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
	private int localValue = 0; // line 1
	private int localTimestamp = 0; // line 2
	private int timestamp = 0; // line 3
	private int sequenceNumber = 0; // line 4
	private List<ActorRef> actorRefList;
	private int N;
	private int M = 49; // default to the max number of operations per process

	// pre-calculated values to be written during the put operations
	private Integer[] writeValue;
	// the specific value currently being written
	private int v;
	private int operationsCompleted = 0;
	// buffer to hold the value read during a get operation
	private int readenValue;

	private boolean isCrashed = false;
	private boolean isLaunched = false;
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
		/*8.REQ Upon receiving the LaunchMessage, the process starts executing put and get operations:
			- M put operations with k = 1 and v = i, N+i, 2N+i, ... MN+i
			- M get operations for k = 1 */
		startOperation();
	}

	// line 28
	/*upon received [?,r] the processes responds with
	 the current local value and timestamp */
	public void onReadRequest(ReadRequest message){
		if(isCrashed)	return;
		// line 29
		getSender().tell(new ProcessMessage(localValue,
						localTimestamp, message.getSequenceNumber()), self());
	}

	// lines 9 to 12 and 18 to 20
	/* Collects local values and timestamps from peers.
	Once a majority is reached, it determines the most recent value
	(i.e., the highest timestamp) and broadcasts the write request. */
	public void onReadResponse(ProcessMessage message) {
		if(isCrashed || !isLaunched)	return;
		if(message.getSequenceNumber() != sequenceNumber)	return;
		int sendValue;
		int sendTimestamp;

		// lines 9 and 18
		readResponses.add(message);

		// majority reached lines 9 and 18
		if(readResponses.size() == N/2 + 1){
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
			// lines 12 and 20
			broadcastMessage(new WriteRequest(sendValue, sendTimestamp));

		}
	}

	// line 23
	/* Upon receiving [v', t'], it updates the local storage if the
	incoming request is newer than the currently stored value.
	It then sends back an ACK for the new variables. */
	public void onWriteRequest(WriteRequest message){
		if(isCrashed)	return;
		int timestampReq = message.getTimestamp();
		int valueReq = message.getValue();

		// line 24
		if((timestampReq > localTimestamp) ||
			(timestampReq == localTimestamp && valueReq > localValue)) 
		{
			localValue = valueReq; // line 25
			localTimestamp = timestampReq; //line 26
		}

		// line 27
		getSender().tell(new Ack(valueReq, timestampReq), self());
	}

	/* Counts ACKs to verify that the value has been safely stored on a majority of nodes.
	 and print the return value*/
	public void onAck(Ack ack){
		if (isCrashed || !isLaunched) return;
		if (ack.getTimestamp() != currentOpTimestamp)	return;

		ackCount++;

		// lines 13 and 21
        if (ackCount == (N / 2) + 1) {
			long timeSpent = System.nanoTime() - operationStartTime;
			if (isWrite) {
				// line 14
            	log.info(processName + ": " + "Put value: " + v + " operation duration: " + timeSpent +"ns");
			} else {
				//line 22
				log.info(processName + ": " + "Get return value: " + readenValue + " operation duration: " + timeSpent +"ns");
			}

            operationsCompleted++;
            ackCount = -1;
			
			/*8.REQ Upon receiving the LaunchMessage, the process starts executing put and get operations:
			- M put operations with k = 1 and v = i, N+i, 2N+i, ... MN+i
			- M get operations for k = 1 */
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
		sequenceNumber++; // lines 7 and 16

		// lines 8 and 17
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
