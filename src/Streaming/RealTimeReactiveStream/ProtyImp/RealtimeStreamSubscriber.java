package Streaming.RealTimeReactiveStream.ProtyImp;

import java.util.BitSet;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.realtime.AbsoluteTime;
import javax.realtime.Clock;
import javax.realtime.PriorityParameters;
import javax.realtime.RelativeTime;

import ExecutionTimeServer.ProcessingGroup;
import RealTimeForkJoinPool.RealtimeForkJoinPool;
import RealTimeForkJoinPool.Policy.SchedulingPolicy;
import Streaming.RealTimeReactiveStream.RealtimeFlow.RealtimeSubscriber;
import Streaming.RealTimeReactiveStream.RealtimeFlow.RealtimeSubscription;
import Streaming.ReusableStream.ReusableReferencePipeline;
import Streaming.ReusableStream.util.ReferencePipelineInitialiser;
import Streaming.ReusableStream.util.ReusbaleStreamCallback;

public class RealtimeStreamSubscriber<T> implements RealtimeSubscriber<T>{
	private final Executor executor; // This is the Executor we'll use to be asynchronous, obeying rule 2.2
	private Subscription standardSubscription; // Obeying rule 3.1, we make this private!
	private boolean done; // It's useful to keep track of whether this Subscriber is done or not
	
	protected PriorityParameters priority = null;
	protected ProcessingGroup server = null;
	private RealtimeSubscription realtimeSubscription;
	protected ReusableReferencePipeline<T> pipeline = null;
	protected ReusbaleStreamCallback<?> callback = null;
	protected int requestSize;
	protected RelativeTime timeout_Interval;
	protected AbsoluteTime nextTimeout;
	protected RealtimeForkJoinPool realtimeForkJoinPool;
	public static AbsoluteTime time;
	public static boolean firstrun=true;
	
	public RealtimeStreamSubscriber(){	this(1024, null, null, null, null);	}
	
	public RealtimeStreamSubscriber(int bufferSize, RelativeTime timeout, ReferencePipelineInitialiser<T> initialiser) {
		this(bufferSize, timeout, null, null, initialiser);
	}

	public RealtimeStreamSubscriber(int bufferSize, RelativeTime timeout, PriorityParameters priority,
			ReferencePipelineInitialiser<T> initialiser){
		this(bufferSize, timeout, priority, null, initialiser);
	}
	
	public RealtimeStreamSubscriber(int bufferSize, RelativeTime timeout, PriorityParameters priority,
			ProcessingGroup server, ReferencePipelineInitialiser<T> initialiser){
		this.pipeline = new ReusableReferencePipeline<T>(true);
		if(initialiser!=null)	initialiser.initialise(pipeline);
		this.requestSize = bufferSize;
		this.timeout_Interval = timeout;
		this.priority = priority;
		this.server = server;
		realtimeForkJoinPool = new RealtimeForkJoinPool(priority, server, SchedulingPolicy.GlobalScheduling);
		executor = new RealtimeForkJoinPool(1, priority, server, SchedulingPolicy.GlobalScheduling);
	}
	public RealtimeStreamSubscriber(int bufferSize, RelativeTime timeout, PriorityParameters priority,
			ProcessingGroup server, ReferencePipelineInitialiser<T> initialiser, BitSet affinity){
		this.pipeline = new ReusableReferencePipeline<T>(true);
		if(initialiser!=null)	initialiser.initialise(pipeline);
		this.requestSize = bufferSize;
		this.timeout_Interval = timeout;
		this.priority = priority;
		this.server = server;
		realtimeForkJoinPool = new RealtimeForkJoinPool(4, priority, server, SchedulingPolicy.GlobalScheduling, affinity);
		executor = new RealtimeForkJoinPool(1, priority, server, SchedulingPolicy.GlobalScheduling, affinity);
	}
	public RealtimeStreamSubscriber(int bufferSize, RelativeTime timeout, PriorityParameters priority,
			ProcessingGroup server, ReferencePipelineInitialiser<T> initialiser, BitSet affinity, int parallelism){
		this.pipeline = new ReusableReferencePipeline<T>(true);
		if(initialiser!=null)	initialiser.initialise(pipeline);
		this.requestSize = bufferSize;
		this.timeout_Interval = timeout;
		this.priority = priority;
		this.server = server;
		realtimeForkJoinPool = new RealtimeForkJoinPool(parallelism, priority, server, SchedulingPolicy.GlobalScheduling, affinity);
		executor = new RealtimeForkJoinPool(1, priority, server, SchedulingPolicy.GlobalScheduling, affinity);
	}
	
	public void setCallback(ReusbaleStreamCallback<?> callback) {
		this.callback = callback;
	}
	// Signal represents the asynchronous protocol between the Publisher and Subscriber
	private static interface Signal {}

	private enum OnComplete implements Signal {Instance;}

	private static class OnError implements Signal {
		public final Throwable error;
		public OnError(final Throwable error) {this.error = error;}
	}

	private static class OnNext<T> implements Signal {
		public final T next;
		public OnNext(final T next) {
			this.next = next;
		}
	}

	private static class OnSubscribe implements Signal {
		public final Subscription subscription;
		public OnSubscribe(final Subscription subscription) {this.subscription = subscription;}
	}
	
	private static class OnRealtimeSubscribe implements Signal {
		public final RealtimeSubscription rtsubscription;
		public OnRealtimeSubscribe(final RealtimeSubscription rtsubscription) {this.rtsubscription = rtsubscription;}
	}
	
  

	// Showcases a convenience method to idempotently marking the Subscriber as "done", so we don't want to process more elements
	// herefor we also need to cancel our `Subscription`.
	private final void done() {
		//On this line we could add a guard against `!done`, but since rule 3.7 says that `Subscription.cancel()` is idempotent, we don't need to.
		done = true; // If `whenNext` throws an exception, let's consider ourselves done (not accepting more elements)
		if (standardSubscription != null) { // If we are bailing out before we got a `Subscription` there's little need for cancelling it.
			try {
				standardSubscription.cancel(); // Cancel the subscription
			} catch(final Throwable t) {
				//Subscription.cancel is not allowed to throw an exception, according to rule 3.15
				(new IllegalStateException(standardSubscription + " violated the Reactive Streams rule 3.15 by throwing an exception from cancel.", t)).printStackTrace(System.err);
			}
		}
	}

	// This method is invoked when the OnNext signals arrive
	// Returns whether more elements are desired or not, and if no more elements are desired,
	// for convenience.
	protected boolean whenNext(final Collection<T> element){
		final Runnable processing = new Runnable() {
			@Override
			public void run() {
				pipeline.processData(element, callback);
			}
		};
		if (pipeline != null && element.size()>0)	realtimeForkJoinPool.submit(processing);
		return true;
	}

	// This method is invoked when the OnComplete signal arrives
	// override this method to implement your own custom onComplete logic.
	protected void whenComplete() { }

	// This method is invoked if the OnError signal arrives
	// override this method to implement your own custom onError logic.
	protected void whenError(Throwable error) { }

	private final void handleOnSubscribe(final Subscription s) {
		if (s == null) {
			// Getting a null `Subscription` here is not valid so lets just ignore it.
		} else if (standardSubscription != null) { // If someone has made a mistake and added this Subscriber multiple times, let's handle it gracefully
			try {
				s.cancel(); // Cancel the additional subscription to follow rule 2.5
			} catch(final Throwable t) {
				//Subscription.cancel is not allowed to throw an exception, according to rule 3.15
				(new IllegalStateException(s + " violated the Reactive Streams rule 3.15 by throwing an exception from cancel.", t)).printStackTrace(System.err);
			}
		} else {
			// We have to assign it locally before we use it, if we want to be a synchronous `Subscriber`
			// Because according to rule 3.10, the Subscription is allowed to call `onNext` synchronously from within `request`
			standardSubscription = s;
			try {
				// If we want elements, according to rule 2.1 we need to call `request`
				// And, according to rule 3.2 we are allowed to call this synchronously from within the `onSubscribe` method
				s.request(1); // Our Subscriber is unbuffered and modest, it requests one element at a time
			} catch(final Throwable t) {
				// Subscription.request is not allowed to throw according to rule 3.16
				(new IllegalStateException(s + " violated the Reactive Streams rule 3.16 by throwing an exception from request.", t)).printStackTrace(System.err);
			}
		}
	}

	private final void handleOnNext(final Collection<T> element) {
		if (!done) { // If we aren't already done
			if(standardSubscription == null && realtimeSubscription==null) { // Technically this check is not needed, since we are expecting Publishers to conform to the spec
				// Check for spec violation of 2.1 and 1.09
				(new IllegalStateException("Someone violated the Reactive Streams rule 1.09 and 2.1 by signalling OnNext before `Subscription.request`. (no Subscription)")).printStackTrace(System.err);
			} else {
				try {
						if (whenNext(element)) {
						try {
							if(realtimeSubscription==null){
								standardSubscription.request(1); // Our Subscriber is unbuffered and modest, it requests one element at a time
							}
							else{ //Once subscribed to a real-time Publisher, then only make a real-time request
								nextTimeout = nextTimeout.add(timeout_Interval);
								realtimeSubscription.requestCollection(requestSize, nextTimeout);
							}
						} catch(final Throwable t) {
							// Subscription.request is not allowed to throw according to rule 3.16
							(new IllegalStateException(standardSubscription + " violated the Reactive Streams rule 3.16 by throwing an exception from request.", t)).printStackTrace(System.err);
						}
					} else {
						done(); // This is legal according to rule 2.6
					}
				} catch(final Throwable t) {
					done();
					try {  
						onError(t);
					} catch(final Throwable t2) {
						//Subscriber.onError is not allowed to throw an exception, according to rule 2.13
						(new IllegalStateException(this + " violated the Reactive Streams rule 2.13 by throwing an exception from onError.", t2)).printStackTrace(System.err);
					}
				}
			}
		}
  }

	// Here it is important that we do not violate 2.2 and 2.3 by calling methods on the `Subscription` or `Publisher`
	private void handleOnComplete() {
		if (standardSubscription == null && realtimeSubscription==null) { // Technically this check is not needed, since we are expecting Publishers to conform to the spec
			// Publisher is not allowed to signal onComplete before onSubscribe according to rule 1.09
			(new IllegalStateException("Publisher violated the Reactive Streams rule 1.09 signalling onComplete prior to onSubscribe.")).printStackTrace(System.err);
		} else {
			done = true; // Obey rule 2.4
			whenComplete();
		}
	}

	// Here it is important that we do not violate 2.2 and 2.3 by calling methods on the `Subscription` or `Publisher`
	private void handleOnError(final Throwable error) {
		if (standardSubscription == null) { // Technically this check is not needed, since we are expecting Publishers to conform to the spec
			// Publisher is not allowed to signal onError before onSubscribe according to rule 1.09
			(new IllegalStateException("Publisher violated the Reactive Streams rule 1.09 signalling onError prior to onSubscribe.")).printStackTrace(System.err);
		} else {
			done = true; // Obey rule 2.4
			whenError(error);
		}
	}

	// We implement the OnX methods on `Subscriber` to send Signals that we will process asycnhronously, but only one at a time

	@Override public void onSubscribe(final Subscription s) {
		// As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `Subscription` is `null`
		if (s == null) throw null;
		signal(new OnSubscribe(s));
	}

	@Override public void onNext(Collection<T> element) {
		// As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `element` is `null`
		if (element == null) throw null;
		AbsoluteTime now = Clock.getRealtimeClock().getTime();
		if (now.compareTo(nextTimeout) < 0) {
			/* indicates returned before expected timeout */
			nextTimeout = now;
		}
		signal(new OnNext<Collection<T>>(element));
	}

	@Override public void onError(final Throwable t) {
		// As per rule 2.13, we need to throw a `java.lang.NullPointerException` if the `Throwable` is `null`
		if (t == null) throw null;
		signal(new OnError(t));
	}

	@Override public void onComplete() {
		signal(OnComplete.Instance);
	}

	// This `ConcurrentLinkedQueue` will track signals that are sent to this `Subscriber`, like `OnComplete` and `OnNext` ,
	// and obeying rule 2.11
	private final ConcurrentLinkedQueue<Signal> inboundSignals = new ConcurrentLinkedQueue<Signal>();

	// We are using this `AtomicBoolean` to make sure that this `Subscriber` doesn't run concurrently with itself,
	// obeying rule 2.7 and 2.11
	private final AtomicBoolean on = new AtomicBoolean(false);


	// What `signal` does is that it sends signals to the `Subscription` asynchronously
	private void signal(final Signal signal) {
	  if (inboundSignals.offer(signal)) // No need to null-check here as ConcurrentLinkedQueue does this for us
		  tryScheduleToExecute(); // Then we try to schedule it for execution, if it isn't already
	}

	private final Runnable handleSingnal = new Runnable() {
		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			if(on.get()) { // establishes a happens-before relationship with the end of the previous run
		      try {
		        final Signal s = inboundSignals.poll(); // We take a signal off the queue
		        if (!done) { // If we're done, we shouldn't process any more signals, obeying rule 2.8
		          // Below we simply unpack the `Signal`s and invoke the corresponding methods
		          if (s instanceof OnNext<?>)
		            handleOnNext(((OnNext<Collection<T>>)s).next);
		          else if (s instanceof OnSubscribe)
		            handleOnSubscribe(((OnSubscribe)s).subscription);
		          else if (s instanceof OnRealtimeSubscribe)
		        	handleRealtimeOnSubscribe(((OnRealtimeSubscribe)s).rtsubscription);
		          else if (s instanceof OnError) // We are always able to handle OnError, obeying rule 2.10
		            handleOnError(((OnError)s).error);
		          else if (s == OnComplete.Instance) // We are always able to handle OnError, obeying rule 2.9
		            handleOnComplete();
		        }
		      } finally {
		        on.set(false); // establishes a happens-before relationship with the beginning of the next run
		        if(!inboundSignals.isEmpty()) // If we still have signals to process
		          tryScheduleToExecute(); // Then we try to schedule ourselves to execute again
		      }
			}
		}
	};
	// This method makes sure that this `Subscriber` is only executing on one Thread at a time
	private final void tryScheduleToExecute() {
		if(on.compareAndSet(false, true)) {
			try {
				executor.execute(handleSingnal);
			} catch(Throwable t) { // If we can't run on the `Executor`, we need to fail gracefully and not violate rule 2.13
				if (!done) {
					try {
						done(); // First of all, this failure is not recoverable, so we need to cancel our subscription
					} finally {
						inboundSignals.clear(); // We're not going to need these anymore
						// This subscription is cancelled by now, but letting the Subscriber become schedulable again means
						// that we can drain the inboundSignals queue if anything arrives after clearing
						on.set(false);
					}
				}
			}
		}
	}
  	/* *********************************************************************************
  	 * 							Real-time Implementation
  	 * *********************************************************************************/
	@Override
  	public void onSubscribe(RealtimeSubscription subscription) {
		if (subscription == null) throw null;
	    signal(new OnRealtimeSubscribe(subscription));
	}
	private final void handleRealtimeOnSubscribe(final RealtimeSubscription rts) {
		if (rts != null) {
			if (standardSubscription != null) {
				/* If someone has made added this Subscriber to a Standard Publisher,
				 * cancel it, and start to use real-time publisher*/
				try {	standardSubscription.cancel();} catch(final Throwable t) {}
			}
			if (realtimeSubscription != null) { // If someone has made a mistake and added this Subscriber multiple times, let's handle it gracefully
				try {
					rts.cancel(); // Cancel the additional subscription to follow rule 2.5
				} catch(final Throwable t) {
					//Subscription.cancel is not allowed to throw an exception, according to rule 3.15
					(new IllegalStateException(rts + " violated the Reactive Streams rule 3.15 by throwing an exception from cancel.", t)).printStackTrace(System.err);
				}
				
			} 
			else {
				// We have to assign it locally before we use it, if we want to be a synchronous `Subscriber`
				// Because according to rule 3.10, the Subscription is allowed to call `onNext` synchronously from within `request`
				realtimeSubscription = rts;
				try {
					// If we want elements, according to rule 2.1 we need to call `request`
					// And, according to rule 3.2 we are allowed to call this synchronously from within the `onSubscribe` method
					synchronized(RealtimeStreamSubscriber.class){
						if(firstrun){ 
							time= Clock.getRealtimeClock().getTime(); 
							firstrun=false;
						}
					}
					nextTimeout = time;//now
					nextTimeout = nextTimeout.add(timeout_Interval);
					rts.requestCollection(requestSize, nextTimeout); // make a real-time request for a collection
				} catch(final Throwable t) {
					// Subscription.request is not allowed to throw according to rule 3.16
					(new IllegalStateException(rts + " violated the Reactive Streams rule 3.16 by throwing an exception from request.", t)).printStackTrace(System.err);
				}
			}
		} else {/* Getting a null `Subscription` here is not valid so lets just ignore it.*/}
	}
}