package ExecutionTimeServer;

import javax.realtime.PriorityParameters;
import javax.realtime.ProcessingGroupParameters;

public class AperiodicServer extends ProcessingGroup {
	private PriorityParameters SP;
	
	AperiodicServer(ProcessingGroupParameters PGP, PriorityParameters SP) {
		super(PGP);
		this.SP=SP;
		//let the handler runs at the given priority level
		if(this.getPGP().getCostOverrunHandler()!=null){
			this.getPGP().getCostOverrunHandler().setSchedulingParameters(this.SP);
		}
		if(this.getPGP().getDeadlineMissHandler()!=null){
			this.getPGP().getDeadlineMissHandler().setSchedulingParameters(this.SP);
		}
	}
	
	/** 
	 * return the priority level of this server
	 * */
	public PriorityParameters getPP() {
		return SP;
	}
}
