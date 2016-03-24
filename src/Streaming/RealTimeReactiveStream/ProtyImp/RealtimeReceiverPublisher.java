package Streaming.RealTimeReactiveStream.ProtyImp;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.realtime.AbsoluteTime;
import javax.realtime.AsyncEventHandler;
import javax.realtime.OneShotTimer;
import javax.realtime.PriorityParameters;

import ExecutionTimeServer.ProcessingGroup;
import RealTimeForkJoinPool.RealtimeForkJoinPool;
import Streaming.RealTimeReactiveStream.RealtimeFlow.RealtimePublisher;
import Streaming.RealTimeReactiveStream.RealtimeFlow.RealtimeSubscriber;
import Streaming.RealTimeReactiveStream.RealtimeFlow.RealtimeSubscription;

public class RealtimeReceiverPublisher<T> implements RealtimePublisher<T> {
	private final Executor executor; // This is our thread pool, which will make sure that our Publisher runs asynchronously to its Subscribers
	private final int DEFAULT_BATCHSIZE = 1024; // In general, if one uses an `Executor`, one should be nice nad not hog a thread for too long, this is the cap for that, in elements
	private long maxPublishSize = Integer.MAX_VALUE;
	protected boolean endOfData = false;
	public boolean failed = false;
	private boolean infinite = true;

	protected ConcurrentLinkedQueue<T> buffer = new ConcurrentLinkedQueue<>();
	protected int requestSize = Integer.MAX_VALUE;
	protected PriorityParameters priority = null;
	protected ProcessingGroup server = null;
	
	private final ArrayList<RealtimeRequest> realtimeRequests = new ArrayList<>();
	private int minRequestSize=Integer.MAX_VALUE;
	private RealtimeSubscriptionImpl subscription = null;
	private RealtimeRequest rtMinRequest = null;
	private AbsoluteTime earliestTimeout = null;
	private RealtimeRequest rtEarlistRequest = null;
	private OneShotTimer timer = null;

	public RealtimeReceiverPublisher() {
		this(null, null);
	}

	public RealtimeReceiverPublisher(PriorityParameters priority, ProcessingGroup server) {
		this.priority = priority;
		this.server = server;
		executor = new RealtimeForkJoinPool(1, priority, server, null);
	}

	public RealtimeReceiverPublisher(PriorityParameters priority, ProcessingGroup server, BitSet affinity) {
		this.priority = priority;
		this.server = server;
		executor = new RealtimeForkJoinPool(1, priority, server, null, affinity);
	}
	
	public void setMaxPublishSize(long maxPublishSize) {
		this.maxPublishSize = maxPublishSize;
		infinite = false;
	}

	@SuppressWarnings("unchecked")
	protected void store(T t) {
		synchronized (buffer) {
			buffer.add(t);
			//
			if (buffer.size() >= minRequestSize) {
				if (rtMinRequest !=null){
					subscription = (RealtimeSubscriptionImpl) rtMinRequest.s;
					final Runnable sendDataNow = new Runnable() {
						@Override	public void run() {
							if(subscription!=null)	subscription.sendCollection(rtMinRequest);
					}};
					executor.execute(sendDataNow);
				}
			}
		}
	}

	protected void end() {
		endOfData = true;
	}

	public void getState() {
		if (failed)
			throw new RuntimeException("Error state signal!");
	}

	@Override
	public void subscribe(final Subscriber<? super Collection<T>> s) {
		// As per rule 1.11, we have decided to support multiple subscribers in a unicast configuration
		// for this `Publisher` implementation.
		// As per 2.13, this method must return normally (i.e. not throw)
		new StandardSubscription(s).init();
	}

	// These represent the protocol of the `AsyncIterablePublishers` SubscriptionImpls
	static interface Signal {};
	enum Cancel implements Signal { Instance; };
	enum Subscribe implements Signal { Instance; };
	enum Send implements Signal { Instance; };
	
	static final class Request implements Signal {
		final long n;
		Request(final long n) {
			this.n = n;
		}
	};
	
	static final class RealtimeRequest implements Signal {
		final int n;
		final AbsoluteTime timeout;
		final RealtimeSubscription s;
		final String id;
		RealtimeRequest(final int n, AbsoluteTime timeout, RealtimeSubscription s, String id) {
			this.n = n;
			this.timeout = timeout;
			this.s = s;
			this.id = id;
		}
	};
	
	/*-----------------------Standard Subscription Implementation-----------------------------------*/
	
  // This is our implementation of the Reactive Streams `Subscription`,
  // which represents the association between a `Publisher` and a `Subscriber`.
  final class StandardSubscription implements Subscription {
    final Subscriber<? super Collection<T>> subscriber; // We need a reference to the `Subscriber` so we can talk to it
    private boolean cancelled = false; // This flag will track whether this `Subscription` is to be considered cancelled or not
    private long demand = 0; // Here we track the current demand, i.e. what has been requested but not yet delivered

    StandardSubscription(final Subscriber<? super Collection<T>> subscriber) {
      // As per rule 1.09, we need to throw a `java.lang.NullPointerException` if the `Subscriber` is `null`
      if (subscriber == null) throw null;
      this.subscriber = subscriber;
    }

    // This `ConcurrentLinkedQueue` will track signals that are sent to this `Subscription`, like `request` and `cancel`
    private final ConcurrentLinkedQueue<Signal> inboundSignals = new ConcurrentLinkedQueue<Signal>();

    // We are using this `AtomicBoolean` to make sure that this `Subscription` doesn't run concurrently with itself,
    // which would violate rule 1.3 among others (no concurrent notifications).
    private final AtomicBoolean on = new AtomicBoolean(false);

    // This method will register inbound demand from our `Subscriber` and validate it against rule 3.9 and rule 3.17
    private void doRequest(final long n) {
      if (n < 1)
        terminateDueTo(new IllegalArgumentException(subscriber + " violated the Reactive Streams rule 3.9 by requesting a non-positive number of elements."));
      else if (demand + n < 1) {
        // As governed by rule 3.17, when demand overflows `Long.MAX_VALUE` we treat the signalled demand as "effectively unbounded"
        demand = Long.MAX_VALUE;  // Here we protect from the overflow and treat it as "effectively unbounded"
        doSend(); // Then we proceed with sending data downstream
      } else {
        demand += n; // Here we record the downstream demand
        doSend(); // Then we can proceed with sending data downstream
      }
    }

    // This handles cancellation requests, and is idempotent, thread-safe and not synchronously performing heavy computations as specified in rule 3.5
    private void doCancel() {
      cancelled = true;
    }

    // Instead of executing `subscriber.onSubscribe` synchronously from within `Publisher.subscribe`
    // we execute it asynchronously, this is to avoid executing the user code (`Iterable.iterator`) on the calling thread.
    // It also makes it easier to follow rule 1.9
    private void doSubscribe() {
      try {
    	  RealtimeReceiverPublisher.this.getState();
      } catch(final Throwable t) {
        subscriber.onSubscribe(new Subscription() { // We need to make sure we signal onSubscribe before onError, obeying rule 1.9
          @Override public void cancel() {}
          @Override public void request(long n) {}
        });
        terminateDueTo(t); // Here we send onError, obeying rule 1.09
      }

      if (!cancelled) {
        // Deal with setting up the subscription with the subscriber
        try {
          subscriber.onSubscribe(this);
        } catch(final Throwable t) { // Due diligence to obey 2.13
          terminateDueTo(new IllegalStateException(subscriber + " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.", t));
        }
      }
    }

    // This is our behavior for producing elements downstream
    private void doSend() {
      try {
        // In order to play nice with the `Executor` we will only send at-most `batchSize` before
        // rescheduing ourselves and relinquishing the current thread.
        int leftInBatch = DEFAULT_BATCHSIZE;
        do {
          Collection<T> next = stripAll();
          subscriber.onNext(next); // Then we signal the next element downstream to the `Subscriber`
          if(!infinite)	if(--maxPublishSize<=0) endOfData =true; //for test a fixed-size data source
          ////////////////////////
          if(endOfData) { // If we are at End-of-Stream
            doCancel(); // We need to consider this `Subscription` as cancelled as per rule 1.6
            subscriber.onComplete(); // Then we signal `onComplete` as per rule 1.2 and 1.5
          }
        } while (!cancelled           // This makes sure that rule 1.8 is upheld, i.e. we need to stop signalling "eventually"
                 && --leftInBatch > 0 // This makes sure that we only send `batchSize` number of elements in one go (so we can yield to other Runnables)
                 && --demand > 0);    // This makes sure that rule 1.1 is upheld (sending more than was demanded)

        if (!cancelled && demand > 0) // If the `Subscription` is still alive and well, and we have demand to satisfy, we signal ourselves to send more data
          signal(Send.Instance);
      } catch(final Throwable t) {
        // We can only get here if `onNext` or `onComplete` threw, and they are not allowed to according to 2.13, so we can only cancel and log here.
        doCancel(); // Make sure that we are cancelled, since we cannot do anything else since the `Subscriber` is faulty.
        (new IllegalStateException(subscriber + " violated the Reactive Streams rule 2.13 by throwing an exception from onNext or onComplete.", t)).printStackTrace(System.err);
      }
    }
    
	private Collection<T> stripAll(){
		Collection<T> res;
		synchronized (buffer) {
			//use ArrayList to enable random access when further process
			res = new ArrayList<>(buffer.size());
			/* --------------- o(n + n) ---------------*/
			for(int i=0; i< buffer.size() ;i++){
				res.add(buffer.poll());
			}// here is not efficient, better to operation directly on the Linked list. Can get o(n+1)
		}
		return res;
	}

    // This is a helper method to ensure that we always `cancel` when we signal `onError` as per rule 1.6
    private void terminateDueTo(final Throwable t) {
      cancelled = true; // When we signal onError, the subscription must be considered as cancelled, as per rule 1.6
      try {
        subscriber.onError(t); // Then we signal the error downstream, to the `Subscriber`
      } catch(final Throwable t2) { // If `onError` throws an exception, this is a spec violation according to rule 1.9, and all we can do is to log it.
        (new IllegalStateException(subscriber + " violated the Reactive Streams rule 2.13 by throwing an exception from onError.", t2)).printStackTrace(System.err);
      }
    }

    // What `signal` does is that it sends signals to the `Subscription` asynchronously
    private void signal(final Signal signal) {
      if (inboundSignals.offer(signal)) // No need to null-check here as ConcurrentLinkedQueue does this for us
        tryScheduleToExecute(); // Then we try to schedule it for execution, if it isn't already
    }

    // This method makes sure that this `Subscription` is only running on one Thread at a time,
    // this is important to make sure that we follow rule 1.3
    private final Runnable handleRequest = new Runnable() {
		@Override
		public void run() {
			if(on.get()) { // establishes a happens-before relationship with the end of the previous run
		        try {
		          final Signal s = inboundSignals.poll(); // We take a signal off the queue
		          if (!cancelled) { // to make sure that we follow rule 1.8, 3.6 and 3.7

		            // Below we simply unpack the `Signal`s and invoke the corresponding methods
		            if (s instanceof Request)
		              doRequest(((Request)s).n);
		            else if (s == Send.Instance)
		              doSend();
		            else if (s == Cancel.Instance)
		              doCancel();
		            else if (s == Subscribe.Instance)
		              doSubscribe();
		          }
		        } finally {
		          on.set(false); // establishes a happens-before relationship with the beginning of the next run
		          if(!inboundSignals.isEmpty()) // If we still have signals to process
		            tryScheduleToExecute(); // Then we try to schedule ourselves to execute again
		        }
		      }
		}
    };
    private final void tryScheduleToExecute() {
      if(on.compareAndSet(false, true)) {
        try {
        	executor.execute(handleRequest);
        } catch(Throwable t) { // If we can't run on the `Executor`, we need to fail gracefully
          if (!cancelled) {
            doCancel(); // First of all, this failure is not recoverable, so we need to follow rule 1.4 and 1.6
            try {
              terminateDueTo(new IllegalStateException("Publisher terminated due to unavailable Executor.", t));
            } finally {
              inboundSignals.clear(); // We're not going to need these anymore
              // This subscription is cancelled by now, but letting it become schedulable again means
              // that we can drain the inboundSignals queue if anything arrives after clearing
              on.set(false);
            }
          }
        }
      }
    }

    // Our implementation of `Subscription.request` sends a signal to the Subscription that more elements are in demand
    @Override public void request(final long n) {
      signal(new Request(n));
    }
    // Our implementation of `Subscription.cancel` sends a signal to the Subscription that the `Subscriber` is not interested in any more elements
    @Override public void cancel() {
      signal(Cancel.Instance);
    }
    // The reason for the `init` method is that we want to ensure the `SubscriptionImpl`
    // is completely constructed before it is exposed to the thread pool, therefor this
    // method is only intended to be invoked once, and immediately after the constructor has
    // finished.
    void init() {
      signal(Subscribe.Instance);
    }
  }

  	/* ***************************************************************************
  	 * 						Real-time Subscription Implementation				 *
  	 * ***************************************************************************/
	@Override
	public void subscribe(RealtimeSubscriber<? super T> RTsubscriber) {
		new RealtimeSubscriptionImpl(RTsubscriber).init();
	};
	
	final class RealtimeSubscriptionImpl implements RealtimeSubscription {
	    final RealtimeSubscriber<? super T> subscriber; // We need a reference to the `Subscriber` so we can talk to it
	    private boolean cancelled = false; // This flag will track whether this `Subscription` is to be considered cancelled or not

	    RealtimeSubscriptionImpl(RealtimeSubscriber<? super T> subscriber) {
	      // As per rule 1.09, we need to throw a `java.lang.NullPointerException` if the `Subscriber` is `null`
	      if (subscriber == null) throw null;
	      this.subscriber = subscriber;
	    }

	    // This `ConcurrentLinkedQueue` will track signals that are sent to this `Subscription`, like `request` and `cancel`
	    private final ConcurrentLinkedQueue<Signal> inboundSignals = new ConcurrentLinkedQueue<Signal>();

	    // We are using this `AtomicBoolean` to make sure that this `Subscription` doesn't run concurrently with itself,
	    // which would violate rule 1.3 among others (no concurrent notifications).
	    private final AtomicBoolean on = new AtomicBoolean(false);

	    private void processRealtimeRequest(RealtimeRequest request) {
	    	synchronized(realtimeRequests){
	    		realtimeRequests.add(request);
	    	}
	    	resetTimerAndRequest();
	    }
	    
	    private void resetTimerAndRequest(){
			synchronized(realtimeRequests){
				minRequestSize = Integer.MAX_VALUE;
				subscription = null;
				rtMinRequest= null;
				earliestTimeout = null;
				rtEarlistRequest = null;
				realtimeRequests.stream().forEach(x -> {
					/* get minimum request */
					if (minRequestSize > x.n) {
						rtMinRequest = x;
						minRequestSize = rtMinRequest.n;
					}
					/* get earliest timeout */
					if (earliestTimeout == null) {
						rtEarlistRequest = x;
						earliestTimeout = rtEarlistRequest.timeout;

					} else {
						if (earliestTimeout.compareTo(x.timeout) > 0) {
							rtEarlistRequest = x;
							earliestTimeout = rtEarlistRequest.timeout;
						}
					}
				});
				if(timer!=null){
					try{timer.destroy();}catch(Exception e){}
				}
				if(earliestTimeout == null){	timer=null;	return;	}
				timer = new OneShotTimer(earliestTimeout, new AsyncEventHandler(priority, null, null, null, null, false, null){
		    		@Override
		    		public void handleAsyncEvent() {
		    			if(rtEarlistRequest!=null){
			    			@SuppressWarnings("unchecked")
							RealtimeSubscriptionImpl rts = (RealtimeSubscriptionImpl) rtEarlistRequest.s;
			    			if(rts!=null)	rts.sendCollection(rtEarlistRequest);
		    			}
		    		}
		    	});
		    	timer.start();
			}
	    }
	    
	    @SuppressWarnings("unchecked")
		public void sendCollection(RealtimeRequest request) {
	    	boolean notYetProcessed = false;
	    	synchronized(realtimeRequests){	
				notYetProcessed = realtimeRequests.remove(request);
	    		resetTimerAndRequest();
	    	}

	    	if(notYetProcessed){
		    	int size = request.n;
		    	try {
	    			Collection<T> next = stripData(size);
	    			subscriber.onNext(next.getClass().cast(next)); // Then we signal the next element downstream to the `Subscriber`
	    			if(!infinite)	if(--maxPublishSize<=0) endOfData =true; //for test a fixed-size data source
	    			if(endOfData) { // If we are at End-of-Stream
	    				doCancel(); // We need to consider this `Subscription` as cancelled as per rule 1.6
	    				subscriber.onComplete(); // Then we signal `onComplete` as per rule 1.2 and 1.5
	    			}
		    	} catch(Exception e) {
		    		// We can only get here if `onNext` or `onComplete` threw, and they are not allowed to according to 2.13, so we can only cancel and log here.
		    		doCancel(); // Make sure that we are cancelled, since we cannot do anything else since the `Subscriber` is faulty.
		    		e.printStackTrace();
		    	}
	    	}
	    }

	    // This handles cancellation requests, and is idempotent, thread-safe and not synchronously performing heavy computations as specified in rule 3.5
	    private void doCancel() {
	      cancelled = true;
	    }

	    // Instead of executing `subscriber.onSubscribe` synchronously from within `Publisher.subscribe`
	    // we execute it asynchronously, this is to avoid executing the user code (`Iterable.iterator`) on the calling thread.
	    // It also makes it easier to follow rule 1.9
	    private void doRealtimeSubscribe() {
	      try {
	    	  RealtimeReceiverPublisher.this.getState();
	      } catch(final Throwable t) {
	        subscriber.onSubscribe(new Subscription() { // We need to make sure we signal onSubscribe before onError, obeying rule 1.9
	          @Override public void cancel() {}
	          @Override public void request(long n) {}
	        });
	        terminateDueTo(t); // Here we send onError, obeying rule 1.09
	      }

	      if (!cancelled) {
	        // Deal with setting up the subscription with the subscriber
	        try {
	          subscriber.onSubscribe(this);
	        } catch(final Throwable t) { // Due diligence to obey 2.13
	          terminateDueTo(new IllegalStateException(subscriber + " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.", t));
	        }
	      }
	    }
	    // TODO
		private Collection<T> stripData(int size){
			if(buffer==null) return new ArrayList<>();
			if(buffer.size() < size)	size = buffer.size();
			Collection<T> res = new ArrayList<>(size);
			/* --------------- o(n + n) ---------------*/
			for(int i=0; i< size ;i++){
				T t;
				try{t= buffer.poll();}catch(Exception ei){
					continue;
				}
				res.add(t);
			}// here is not efficient, better to operation directly on the Linked list. Can get o(n+1)
			return res;
		}

	    // This is a helper method to ensure that we always `cancel` when we signal `onError` as per rule 1.6
	    private void terminateDueTo(final Throwable t) {
	      cancelled = true; // When we signal onError, the subscription must be considered as cancelled, as per rule 1.6
	      try {
	        subscriber.onError(t); // Then we signal the error downstream, to the `Subscriber`
	      } catch(final Throwable t2) { // If `onError` throws an exception, this is a spec violation according to rule 1.9, and all we can do is to log it.
	        (new IllegalStateException(subscriber + " violated the Reactive Streams rule 2.13 by throwing an exception from onError.", t2)).printStackTrace(System.err);
	      }
	    }

	    // What `signal` does is that it sends signals to the `Subscription` asynchronously
	    private void signal(final Signal signal) {
	      if (inboundSignals.offer(signal)) // No need to null-check here as ConcurrentLinkedQueue does this for us
	        tryScheduleToExecute(); // Then we try to schedule it for execution, if it isn't already
	    }

	    // This method makes sure that this `Subscription` is only running on one Thread at a time,
	    // this is important to make sure that we follow rule 1.3
	    private final Runnable handleRealtimeSubscription = new Runnable() {
			@Override
			public void run() {
				if(on.get()) { // establishes a happens-before relationship with the end of the previous run
			        try {
			          final Signal s = inboundSignals.poll(); // We take a signal off the queue
			          if (!cancelled) { // to make sure that we follow rule 1.8, 3.6 and 3.7

			            // Below we simply unpack the `Signal`s and invoke the corresponding methods
			            if (s instanceof RealtimeRequest)
			            	processRealtimeRequest((RealtimeRequest)s);
			            else if (s == Cancel.Instance)
			            	doCancel();
			            else if (s == Subscribe.Instance)
			            	doRealtimeSubscribe();
			          }
			        } finally {
			          on.set(false); // establishes a happens-before relationship with the beginning of the next run
			          if(!inboundSignals.isEmpty()) // If we still have signals to process
			            tryScheduleToExecute(); // Then we try to schedule ourselves to execute again
			        }
			      }
			}
    	};
	    private final void tryScheduleToExecute() {
	      if(on.compareAndSet(false, true)) {
	        try {
	        	executor.execute(handleRealtimeSubscription);
	        } catch(Throwable t) { // If we can't run on the `Executor`, we need to fail gracefully
	          if (!cancelled) {
	            doCancel(); // First of all, this failure is not recoverable, so we need to follow rule 1.4 and 1.6
	            try {
	              terminateDueTo(new IllegalStateException("Publisher terminated due to unavailable Executor.", t));
	            } finally {
	              inboundSignals.clear(); // We're not going to need these anymore
	              // This subscription is cancelled by now, but letting it become schedulable again means
	              // that we can drain the inboundSignals queue if anything arrives after clearing
	              on.set(false);
	            }
	          }
	        }
	      }
	    }
	    // Our implementation of `Subscription.cancel` sends a signal to the Subscription that the `Subscriber` is not interested in any more elements
	    @Override public void cancel() {
	      signal(Cancel.Instance);
	    }
	    // The reason for the `init` method is that we want to ensure the `SubscriptionImpl`
	    // is completely constructed before it is exposed to the thread pool, therefor this
	    // method is only intended to be invoked once, and immediately after the constructor has
	    // finished.
	    void init() {
	      signal(Subscribe.Instance);
	    }

		@Override
		public void requestCollection(int size, AbsoluteTime timeout) {
			RealtimeRequest s = new RealtimeRequest(size,timeout,this,Thread.currentThread().getName());
			signal(s);
		}
	  }
}