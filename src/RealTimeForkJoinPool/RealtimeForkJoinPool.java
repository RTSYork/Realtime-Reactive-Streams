package RealTimeForkJoinPool;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.BitSet;
import java.util.concurrent.ForkJoinPool;

import javax.realtime.PriorityParameters;
import javax.realtime.PriorityScheduler;

import ExecutionTimeServer.ProcessingGroup;
import RealTimeForkJoinPool.Policy.SchedulingPolicy;

public class RealtimeForkJoinPool extends ForkJoinPool {
	
	public static final int minPriority=1;
	public static final int maxPriority=PriorityScheduler.instance().getMaxPriority();
	public static final int defaultPriority=PriorityScheduler.instance().getMinPriority();
	
	
	//constructor
	public RealtimeForkJoinPool() {
		this(Runtime.getRuntime().availableProcessors(), new RealtimeForkJoinWorkerThreadFactory(
				new PriorityParameters(defaultPriority), null, SchedulingPolicy.PartitionedScheduling), 
				null, false);
	}
	
	public RealtimeForkJoinPool(PriorityParameters priority){
		this(Runtime.getRuntime().availableProcessors(),
				new RealtimeForkJoinWorkerThreadFactory(priority,null,SchedulingPolicy.PartitionedScheduling),
				null,
				false);
	}
	
	public RealtimeForkJoinPool(PriorityParameters priority, ProcessingGroup server){
		this(Runtime.getRuntime().availableProcessors(),
				new RealtimeForkJoinWorkerThreadFactory(priority,server,SchedulingPolicy.PartitionedScheduling),
				null,
				false);
	}
	
	public RealtimeForkJoinPool(PriorityParameters priority, ProcessingGroup server, SchedulingPolicy schdlPolicy){
		this(Runtime.getRuntime().availableProcessors(),
				new RealtimeForkJoinWorkerThreadFactory(priority,server,schdlPolicy),
				null,
				false);
	}
	
	public RealtimeForkJoinPool(int parallelism, PriorityParameters priority, ProcessingGroup server, SchedulingPolicy schdlPolicy){
		this(parallelism,
				new RealtimeForkJoinWorkerThreadFactory(priority,server,schdlPolicy),
				null,
				false);
	}
	public RealtimeForkJoinPool(int parallelism, PriorityParameters priority, ProcessingGroup server, SchedulingPolicy schdlPolicy, BitSet affinity){
		this(parallelism,
				new RealtimeForkJoinWorkerThreadFactory(priority,server,affinity),
				null,
				false);
	}
	
	public RealtimeForkJoinPool(int parallelism,
            ForkJoinWorkerThreadFactory factory,
            UncaughtExceptionHandler handler,
            boolean asyncMode) {
		super(parallelism,factory,handler,asyncMode);
		/* Population of worker threads */
		//this.createAllWorkerThreads();
	}
	
	/**
	 * Return a pre-defined Real-time ForkJoinPool with a given priority,
	 * where all the worker threads are aperiodic threads.
	 * Return a commoon pool with Java priority if the given priority is not in
	 * the range of RTSJ.
	 */
	public static ForkJoinPool getRTForkJoinPool(PriorityParameters priorityPmtr){
		int priority=priorityPmtr.getPriority();
		return getCommonPool(priority);
	}
}
