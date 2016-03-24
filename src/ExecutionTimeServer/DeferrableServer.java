package ExecutionTimeServer;

import java.util.HashMap;

import javax.realtime.AsyncEventHandler;
import javax.realtime.PeriodicTimer;
import javax.realtime.PriorityParameters;
import javax.realtime.ProcessingGroupParameters;
import javax.realtime.RealtimeThread;
/**
 * DeferrableServer allows multiple threads to register to itself,
 * so that EACH of the threads execute certain amount time per period.
 * For example, T1, and T2 registered in a DeferrableServer(capacity=10s, period=100s):
 * T2 has executed 10s, T1 can execute its own 10s without any impact from T2.
 * */
public class DeferrableServer extends AperiodicServer {
	private PriorityParameters backgroud;
	//private PeriodicTimer timer;
	private HashMap<RealtimeThread, Integer> originalPrioritys= new HashMap<>();
	
	public DeferrableServer(ProcessingGroupParameters PGP, PriorityParameters SP, PriorityParameters backgroud) {
		super(PGP, SP);
		this.backgroud=backgroud;
		
	}
	
	public PriorityParameters getBackgroud() {
		return backgroud;
	}
	
	/** 
	 * return associatedRealtimeThreads' original priorities
	 * */	
	public HashMap<RealtimeThread, Integer> getOriginalPrioritys() {
		return originalPrioritys;
	}
	
	@Override
	protected void setProcessingGroupParameter(RealtimeThread t, ProcessingGroupParameters p){
		/* get the original CostOverRunHandler from its ProcessingGroupParameters,
		 * constructs a delegating CostOverRunHandler handler,
		 * and replace the original CostOverRunHandler with this delegating handler*/
		AsyncEventHandler originalOverRunHandler = PGP.getCostOverrunHandler();
		ProcessingGroupParameters delegationPGP = new ProcessingGroupParameters(
				p.getStart(),
				p.getPeriod(),
				p.getCost(),
				p.getDeadline(),
				p.getCostOverrunHandler(),
				p.getDeadlineMissHandler());
		delegationPGP.setCostOverrunHandler(new DelegateOverRunHandler(this, originalOverRunHandler, t));
		//start the monitoring timer
		TimerHandler tHandler= new TimerHandler(this, t);
		PeriodicTimer timer=new PeriodicTimer(delegationPGP.getStart(),delegationPGP.getPeriod(), tHandler);
		tHandler.setTimer(timer);
		timer.start();
		t.setProcessingGroupParameters(delegationPGP);
	}
	
	/**
	 * Also remove its original priority from the record
	 * */
	@Override
	public void deRegister(RealtimeThread... rtThreadsToDeregister){
		super.deRegister(rtThreadsToDeregister);
		try{
			for(int i=0;i<rtThreadsToDeregister.length;i++){
				if(rtThreadsToDeregister[i]!=null){
					originalPrioritys.remove(rtThreadsToDeregister[i]);
				}
			}
		}catch(NullPointerException e) {	}
	}
}


//Handler for Timer
class TimerHandler extends AsyncEventHandler {
	
	private DeferrableServer deferrableSvr;
	private PeriodicTimer timer;
	private RealtimeThread target;
	public TimerHandler(DeferrableServer p, RealtimeThread t) {
		//set the handler's priority to the server's priority
		super(p.getPP(),null,null,null,null,false,null);
		this.deferrableSvr=p;
		this.target = t;
	}

	public void setTimer(PeriodicTimer timer) {
		this.timer = timer;
	}

	@Override
	public void handleAsyncEvent() {
		try{
			RealtimeThread targetRTThread = target;
			if(targetRTThread!=null){
				if(targetRTThread.getState()!=Thread.State.TERMINATED){
					if(deferrableSvr.getOriginalPrioritys().get(targetRTThread)!=null){
						int originalPriority=deferrableSvr.getOriginalPrioritys().get(targetRTThread);
						/* Promote the priority to its original priority
						 * if current priority bigger than the original one, indicates the priority is promoted
						 * by resource accessing control protocols, e.g. priority ceiling,
						 * thus, skipped
						 */
						if(((PriorityParameters) targetRTThread.getSchedulingParameters()).getPriority()<originalPriority){
							PriorityParameters priorityp=new PriorityParameters(originalPriority);
							targetRTThread.setSchedulingParameters(priorityp);
						}
					}
				}
				else{	if(timer!=null)	timer.destroy();	}
			}else{	if(timer!=null)	timer.destroy();	}
		}catch(Exception e) {
			System.out.println("Timer in Deferrable Server exception: "+e.getMessage());
		}
	}
}

//delegateOverRunHandler
class DelegateOverRunHandler extends AsyncEventHandler {
	
	private DeferrableServer deferrableSvr;
	private AsyncEventHandler originalOverRunHandler;
	private RealtimeThread target;

	public DelegateOverRunHandler(DeferrableServer ds, AsyncEventHandler originalOverRunHandler, RealtimeThread t) {
		// set the handler's priority to the server's priority
		super(ds.getPP(), null, null, null, null, false, null);
		this.deferrableSvr = ds;
		this.originalOverRunHandler = originalOverRunHandler;
		this.target = t;
	}

	@Override
	public void handleAsyncEvent() {
		// set all threads' priority to MinPriority
		try {
			RealtimeThread targetRTThread = target;
			if (targetRTThread != null) {
				if (targetRTThread.getState() != Thread.State.TERMINATED) {
					// save original priority
					if (deferrableSvr.getOriginalPrioritys().get(targetRTThread) == null) {
						deferrableSvr.getOriginalPrioritys().put(targetRTThread,
								((PriorityParameters) targetRTThread.getSchedulingParameters()).getPriority());
					}
					targetRTThread.setSchedulingParameters(deferrableSvr.getBackgroud());
//					System.out.println("targetRTThread Priority from"+targetRTThread.getPriority()+ " to "+deferrableSvr.getBackgroud().getPriority());
				} else {
					deferrableSvr.deRegister(targetRTThread);
				}
			}
			
			// Fire the original handler
			if (originalOverRunHandler != null) {
				originalOverRunHandler.handleAsyncEvent();
			}
		} catch (Exception e) {
			System.out.println("Deferrable Server Execption:"+e.getMessage());
		}
	}
}