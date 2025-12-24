package code;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class App {
	public static void main(String[] args) {
		final int N = 3; 
		final int f = 1; 

		ActorSystem system = ActorSystem.create("system");
		List<ActorRef> processes = new ArrayList<>();

		// create processes
		for (int i = 0; i < N; i++) 
			processes.add(system.actorOf(Process.createActor(), "p" + i));
				
		// send ref
		ReferencesMessage refs = new ReferencesMessage(processes);
		for (ActorRef p : processes) 
			p.tell(refs, ActorRef.noSender());
		

		// crash
		List<ActorRef> toCrash = new ArrayList<>(processes);
		Collections.shuffle(toCrash);
		
		for (int i = 0; i < f; i++) 
			toCrash.get(i).tell(new CrashMessage(), ActorRef.noSender());
		
		// launch
		LaunchMessage launch = new LaunchMessage();
		// try {			
		// 	Thread.sleep(1000); 
		// } catch (InterruptedException e) { e.printStackTrace(); }
		
		for (int i = f; i < N; i++) 
			toCrash.get(i).tell(launch, ActorRef.noSender());
		
	}
}