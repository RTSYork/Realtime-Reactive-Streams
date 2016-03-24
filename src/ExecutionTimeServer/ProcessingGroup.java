package ExecutionTimeServer;

import java.util.HashMap;
import java.util.LinkedList;

import javax.realtime.ProcessingGroupParameters;
import javax.realtime.RealtimeThread;
import javax.realtime.Schedulable;

public abstract class ProcessingGroup {
	protected ProcessingGroupParameters PGP;
	//Real-time Threads that associated with this server
	protected LinkedList<RealtimeThread> associatedRealtimeThreads = new LinkedList<>();
	protected HashMap<Schedulable,Boolean> associatedSchedulables = new HashMap<>();
	
	ProcessingGroup(ProcessingGroupParameters PGP) {
		this.PGP=PGP;
	}
	
	public ProcessingGroupParameters getPGP() {
		return PGP;
	}
	
	/** 
	 * Register a RealtimeThread to this server, target thread will use 
	 * the ProcessingGroupParameters of this server, through
	 * setProcessingGroupParameters(this.getPGP())
	 * */
	public void register(RealtimeThread... rtThreadsToRegister){
		try{
			for(int i=0;i<rtThreadsToRegister.length;i++){
				if(rtThreadsToRegister[i]!=null){
					if(!associatedSchedulables.containsKey(rtThreadsToRegister[i])){
						associatedSchedulables.put(rtThreadsToRegister[i], true);
						associatedRealtimeThreads.add(rtThreadsToRegister[i]);
						setProcessingGroupParameter(rtThreadsToRegister[i], this.getPGP() );
					}
				}
			}
		}catch(NullPointerException e) {	}
	}
	
	protected void setProcessingGroupParameter(RealtimeThread t, ProcessingGroupParameters p){
		t.setProcessingGroupParameters(p);
	}
	
	/** 
	 * DeRegister a RealtimeThread to this server,
	 * the ProcessingGroupParameters of this server will be detached
	 * */
	public void deRegister(RealtimeThread... rtThreadsToDeregister){
		try{
			for(int i=0;i<rtThreadsToDeregister.length;i++){
				if(rtThreadsToDeregister[i]!=null){
					rtThreadsToDeregister[i].setProcessingGroupParameters(null);
				}
				associatedRealtimeThreads.remove(rtThreadsToDeregister[i]);
			}
		}catch(NullPointerException e) {	}
	}
	
	/** 
	 * return associatedRealtimeThreads
	 * */
	public LinkedList<RealtimeThread> getAssociatedRealtimeThreads(){
		return associatedRealtimeThreads;
	}
}
