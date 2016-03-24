package ExecutionTimeServer;

import javax.realtime.PriorityParameters;
import javax.realtime.ProcessingGroupParameters;

public class PollingServer extends AperiodicServer {

	public PollingServer(ProcessingGroupParameters PGP, PriorityParameters SP) {
		super(PGP, SP);
	}

}
