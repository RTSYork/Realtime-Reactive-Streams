package Streaming.RealTimeReactiveStream.ProtyImp;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.util.BitSet;

import javax.realtime.Affinity;
import javax.realtime.PriorityParameters;
import javax.realtime.PriorityScheduler;
import javax.realtime.RealtimeThread;

import ExecutionTimeServer.ProcessingGroup;

public class RTSocketPublisher extends RealtimeReceiverPublisher<String> {
	private String host = "localhost";
	private int port = 1989;
	private boolean running = false;
	
	private RealtimeThread receiverThread = null;
	private PriorityParameters priority;
	private ProcessingGroup server = null;
	private BitSet affinity = null;

	public RTSocketPublisher(String host, int port) {
		this(host, port, null, null, null);
	}
	
	public RTSocketPublisher(String host, int port, PriorityParameters priority, ProcessingGroup server, BitSet affinity) {
		super(priority, server, affinity);
		this.host = host;
		this.port = port;
		this.server = server;
		this.priority = priority;
		if(this.priority==null){
			this.priority=new PriorityParameters(PriorityScheduler.instance().getMinPriority());
		}
		this.affinity = affinity;
		start();
	}
	
	private void start(){
		if(!running){
			running = true;
			// Start a thread, tries to receive input over a socket connection
			if(receiverThread!=null && server!=null){
				server.deRegister(receiverThread);
			}
			receiverThread=new RealtimeThread(priority) {
				@Override
				public void run() {
					receive();
				}
			};
			if(server!=null) server.register(receiverThread);
			receiverThread.setSchedulingParameters(priority);
			if(affinity!=null){
				try {
					Affinity.set(Affinity.generate(affinity), receiverThread);
				} catch (Exception e) {
					System.out.println("Affinity set failed : " +e.getMessage());
				}
			}
			receiverThread.start();
		}
	}
	
	private void receive() {
		String userInput = null;
		Socket socket = null;
		BufferedReader reader = null;
		try {
			// connects to the server
			socket = new Socket(host, port);
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			//System.out.println("connected...");
			while ((userInput = reader.readLine()) != null) {
				// System.out.println("Received: "+userInput);
				store(userInput);
			}
			reader.close();
			socket.close();
			end();
		} catch (ConnectException ce) {
			if(running)	restart();
			//System.out.println(ce.getMessage());
		} catch (Throwable t) {
			System.out.println(t.getMessage());
		}
	}
	
	public void restart(){
		//System.out.println("restart...");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
		running=false;
		start();
	}
}
