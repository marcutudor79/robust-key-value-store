package keyValueStore;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import keyValueStore.msg.LaunchMessage;
import keyValueStore.msg.OperationsMessage;
import keyValueStore.msg.ProcessMessage;
import keyValueStore.msg.ReadRequest;
import keyValueStore.msg.ReferencesMessage;
import keyValueStore.msg.WriteRequest;
import scala.concurrent.duration.FiniteDuration;

/**
 * Deterministic tests of the real Process actor. The actor under test (p0) is
 * given three replicas r1, r2, r3 played by test probes: the test decides
 * exactly which message each replica sends and when, which is allowed by the
 * model of the assignment (reliable but asynchronous channels: any delay).
 * Every message a replica sends is one a correct replica would send (lines
 * 23-29 of the pseudocode), given what it has received.
 */
public class QuorumAccountingTest {

    static final FiniteDuration QUIET = FiniteDuration.create(500, TimeUnit.MILLISECONDS);

    ActorSystem system;
    TestKit r1, r2, r3, monitor;

    @Before
    public void setUp() {
        system = ActorSystem.create("test");
        r1 = new TestKit(system);
        r2 = new TestKit(system);
        r3 = new TestKit(system);
        monitor = new TestKit(system);
    }

    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(system);
    }

    ActorRef start(String name, int m) {
        ActorRef p = system.actorOf(Process.createActor(), name);
        p.tell(new ReferencesMessage(Arrays.asList(r1.getRef(), r2.getRef(), r3.getRef()), monitor.getRef()), ActorRef.noSender());
        p.tell(new OperationsMessage(m), ActorRef.noSender());
        p.tell(new LaunchMessage(), ActorRef.noSender());
        return p;
    }

    static <T> T expectAll(Class<T> c, TestKit... probes) {
        T first = null;
        for (TestKit p : probes) {
            T msg = p.expectMsgClass(c);
            if (first == null) first = msg;
        }
        return first;
    }

    /**
     * Pseudocode line 13: a write completes once [ack, v, t] for THIS write was
     * received from a majority. Here only r1 has acknowledged (3, 2); the only
     * other ack received is r3's late ack of the PREVIOUS write (0, 1). The
     * write of 3 must not complete yet.
     */
    @Test
    public void staleAckOfPreviousWriteMustNotCountForTheNextOne() {
        ActorRef p0 = start("p0", 2); // put(0), put(3), get, get

        // put(0), phase 1: r1 and r2 answer
        int s1 = expectAll(ReadRequest.class, r1, r2, r3).getSequenceNumber();
        p0.tell(new ProcessMessage(0, 0, s1), r1.getRef());
        p0.tell(new ProcessMessage(0, 0, s1), r2.getRef());
        // put(0), phase 2: all three store (0, 1); r1 and r2 ack, r3's ack is slow
        WriteRequest w1 = expectAll(WriteRequest.class, r1, r2, r3);
        assertEquals(0, w1.getValue());
        assertEquals(1, w1.getTimestamp());
        p0.tell(new Ack(0, 1), r1.getRef());
        p0.tell(new Ack(0, 1), r2.getRef());

        // put(0) completed: put(3), phase 1 starts
        int s2 = expectAll(ReadRequest.class, r1, r2, r3).getSequenceNumber();
        p0.tell(new Ack(0, 1), r3.getRef()); // r3's late ack of put(0) arrives now
        p0.tell(new ProcessMessage(0, 1, s2), r1.getRef());
        p0.tell(new ProcessMessage(0, 1, s2), r2.getRef());
        // put(3), phase 2
        WriteRequest w2 = expectAll(WriteRequest.class, r1, r2, r3);
        assertEquals(3, w2.getValue());
        assertEquals(2, w2.getTimestamp());
        p0.tell(new Ack(3, 2), r1.getRef()); // only r1 has stored (3, 2)

        // One ack for (3, 2) out of three replicas: not a majority.
        // If put(3) completed, p0 would start its next operation (a get).
        r1.expectNoMessage(QUIET);
        r2.expectNoMessage(QUIET);
        r3.expectNoMessage(QUIET);

        // Liveness: a second genuine ack completes it.
        p0.tell(new Ack(3, 2), r2.getRef());
        expectAll(ReadRequest.class, r1, r2, r3);
    }

    /**
     * Requirement 8: exactly M puts and M gets. Once the last get is done, a
     * late third answer or ack must not start another operation.
     */
    @Test
    public void lateMessagesAfterLastOperationStartNothing() {
        ActorRef p0 = start("p0", 1); // put(0), get
        int s = expectAll(ReadRequest.class, r1, r2, r3).getSequenceNumber();
        p0.tell(new ProcessMessage(0, 0, s), r1.getRef());
        p0.tell(new ProcessMessage(0, 0, s), r2.getRef());
        expectAll(WriteRequest.class, r1, r2, r3);
        p0.tell(new Ack(0, 1), r1.getRef());
        p0.tell(new Ack(0, 1), r2.getRef());

        s = expectAll(ReadRequest.class, r1, r2, r3).getSequenceNumber();
        p0.tell(new ProcessMessage(0, 1, s), r1.getRef());
        p0.tell(new ProcessMessage(0, 1, s), r2.getRef());
        expectAll(WriteRequest.class, r1, r2, r3);
        p0.tell(new Ack(0, 1), r1.getRef());
        p0.tell(new Ack(0, 1), r2.getRef());
        monitor.expectMsgClass(keyValueStore.msg.DoneMessage.class);

        p0.tell(new ProcessMessage(0, 1, s), r3.getRef()); // r3's answer to the last get, late
        p0.tell(new Ack(0, 1), r3.getRef()); // and its ack, late
        r1.expectNoMessage(QUIET);
        r2.expectNoMessage(QUIET);
        r3.expectNoMessage(QUIET);
    }

    /**
     * Pseudocode lines 9-12: the write request is sent once per operation,
     * when a majority has answered. A late third answer must not start the
     * write phase again.
     */
    @Test
    public void writePhaseStartsOncePerOperation() {
        ActorRef p0 = start("p0", 1);
        int s = expectAll(ReadRequest.class, r1, r2, r3).getSequenceNumber();
        p0.tell(new ProcessMessage(0, 0, s), r1.getRef());
        p0.tell(new ProcessMessage(0, 0, s), r2.getRef());
        expectAll(WriteRequest.class, r1, r2, r3);
        p0.tell(new ProcessMessage(0, 0, s), r3.getRef()); // the third answer, late

        r1.expectNoMessage(QUIET);
        r2.expectNoMessage(QUIET);
        r3.expectNoMessage(QUIET);
    }
}
