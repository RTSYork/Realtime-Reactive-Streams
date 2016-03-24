package RealTimeForkJoinPool;
import java.util.BitSet;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;

import javax.realtime.Affinity;
import javax.realtime.PriorityParameters;
import javax.realtime.PriorityScheduler;

import ExecutionTimeServer.ProcessingGroup;
import RealTimeForkJoinPool.Policy.SchedulingPolicy;


public class RealtimeForkJoinWorkerThreadFactory implements ForkJoinWorkerThreadFactory {
	private PriorityParameters priority=new PriorityParameters(PriorityScheduler.instance().getMinPriority());
	private ProcessingGroup server=null;
	private int nextAvailableProcessorId=0;
	private int numberOfProcessors=0;
	private int[] availableProcessors;
	private SchedulingPolicy schedulingPolicy=SchedulingPolicy.PartitionedScheduling;
	
	//constructor
	public RealtimeForkJoinWorkerThreadFactory(PriorityParameters priority, ProcessingGroup server){
		this(priority,server,SchedulingPolicy.PartitionedScheduling);
	}
	
	public RealtimeForkJoinWorkerThreadFactory(PriorityParameters priority, ProcessingGroup server, SchedulingPolicy schdlPolicy){
		this.priority=priority;
		this.server=server;
		this.schedulingPolicy=schdlPolicy;
		getAvailableProcessors();
	}
	
	public RealtimeForkJoinWorkerThreadFactory(PriorityParameters priority, ProcessingGroup server, BitSet affinity){
		this.priority=priority;
		this.server=server;
		numberOfProcessors = 0;
		for (int i = 0; i < affinity.size(); i++)
			if (affinity.get(i) == true) numberOfProcessors++;
		//init array
		availableProcessors = new int[numberOfProcessors];
		//set values
		numberOfProcessors=0;
		for (int i = 0; i < affinity.size(); i++)
			if (affinity.get(i) == true) availableProcessors[numberOfProcessors++]=i;
	}
	
    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
    	if(server!=null){
    		//allocates a server for each thread
    		return new RealtimeForkJoinWorkerThread(pool,priority,server, getNextAffinity());
    	}
    	return new RealtimeForkJoinWorkerThread(pool,priority,null, getNextAffinity());
    }
    
    /**
     * Get the available processors, including using the
     * taskset -c
     * command in Linux 
     * */
	private void getAvailableProcessors() {
		BitSet ap = Affinity.getAvailableProcessors();
		//get how many processors are available
		numberOfProcessors = 0;
		for (int i = 0; i < ap.size(); i++)
			if (ap.get(i) == true) numberOfProcessors++;
		//init array
		availableProcessors = new int[numberOfProcessors];
		//set values
		numberOfProcessors=0;
		for (int i = 0; i < ap.size(); i++)
			if (ap.get(i) == true) availableProcessors[numberOfProcessors++]=i;
	}
	
	/**
	 * return null if using global scheduling
	 * */
	
    private Affinity getNextAffinity(){
    	if(this.schedulingPolicy==SchedulingPolicy.GlobalScheduling){
    		return null;
    	}
    	BitSet targetProcessor=new BitSet();
		targetProcessor.set(getNextProcessorIndex());
		return Affinity.generate(targetProcessor);
    }
    
    private synchronized int getNextProcessorIndex(){
    	int processorId=nextAvailableProcessorId;
    	++nextAvailableProcessorId;
    	return availableProcessors[(processorId%numberOfProcessors)];
    }
}