package Streaming.ReusableStream;

import java.util.Collection;

import Streaming.ReusableStream.util.ReusbaleStreamCallback;
/**
 * All the methods that are used for extending Java 8 stream to handle streaming data 
 * are declared here
 * */
public interface ReusableBaseStream<T> {
	/**
	 * Process the data with a stream using all the intermediate 
	 * operations and the terminal operation.  */
	public void processData(Collection<T> data);
	/**
	 * Process the data with a stream using all the intermediate 
	 * operations and the terminal operation.  */
	public void processData(Collection<T> data, ReusbaleStreamCallback<?> resContainer);

}
