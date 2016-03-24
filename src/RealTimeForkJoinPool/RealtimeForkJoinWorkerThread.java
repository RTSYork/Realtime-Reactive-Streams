package RealTimeForkJoinPool;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import javax.realtime.Affinity;
import javax.realtime.PriorityParameters;
import javax.realtime.PriorityScheduler;
import javax.realtime.ProcessorAffinityException;

import ExecutionTimeServer.ProcessingGroup;

public class RealtimeForkJoinWorkerThread extends ForkJoinWorkerThread{

	protected RealtimeForkJoinWorkerThread(ForkJoinPool pool, PriorityParameters priority, ProcessingGroup server, Affinity affinitySet) {
		super(pool);
		if(priority==null)
			priority=new PriorityParameters(PriorityScheduler.instance().getMinPriority());
		this.setSchedulingParameters(priority);
		if(affinitySet!=null){
			try {
				Affinity.set(affinitySet, this);
			} catch (ProcessorAffinityException e) {System.err.println("Affinity.set error in RTForkJoinWorkerThread");}
		}
		if(server!=null){
			server.register(this);
		}
	}
}