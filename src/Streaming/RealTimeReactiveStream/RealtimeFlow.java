package Streaming.RealTimeReactiveStream;

import java.util.Collection;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;

import javax.realtime.AbsoluteTime;

public final class RealtimeFlow {

    private RealtimeFlow() {}
    
    public static interface RealtimePublisher<T> extends Publisher<Collection<T>>{
    	public void subscribe(RealtimeSubscriber<? super T> RTsubscriber);
    }


    public static interface RealtimeSubscriber<T> extends Subscriber<Collection<T>>{
    	/**
         * Method invoked prior to invoking any other Subscriber
         * methods for the given Subscription. If this method throws
         * an exception, resulting behavior is not guaranteed, but may
         * cause the Subscription not to be established or to be cancelled.
         *
         * <p>Typically, implementations of this method invoke {@code
         * subscription.request} to enable receiving items.
         *
         * @param subscription a new subscription
         */
        public void onSubscribe(RealtimeSubscription subscription);
    }

    /**
     * Message control linking a {@link Publisher} and {@link
     * Subscriber}.  Subscribers receive items only when requested,
     * and may cancel at any time. The methods in this interface are
     * intended to be invoked only by their Subscribers; usages in
     * other contexts have undefined effects.
     */
    public static interface RealtimeSubscription{
    	
        /**
         * Adds a reference to a Collection the current
         * unfulfilled demand for this subscription, and desires  
         * the given number {@code n} of items in that collection. 
         * {@code timeout} represents the timeout, when the {@link Subscription} should 
         * pass a collection whose size is less than n. 
         * If {@code n} is negative, the Subscriber will receive an {@code onError}
         * signal with an {@link IllegalArgumentException} argument.
         * Otherwise, the Subscriber will receive a collection that has {@code n}
         * items via {@code onNext(Collection<T>)} invocations (or fewer if
         * timeout occurs).
         * 
         * @param size the desired number of items in the buffer
         * @param timeout the timeout
         */
        public void requestCollection(int size, AbsoluteTime timeout);

        public void cancel();
    }

    /**
     * A component that acts as both a Subscriber and Publisher.
     *
     * @param <T> the subscribed item type
     * @param <R> the published item type
     */
    public static interface RealTimeProcessor<T,R> extends RealtimeSubscriber<T>, RealtimePublisher<R> {
    }

    static final int DEFAULT_BUFFER_SIZE = 256;

    /**
     * Returns a default value for Publisher or Subscriber buffering,
     * that may be used in the absence of other constraints.
     *
     * @implNote
     * The current value returned is 256.
     *
     * @return the buffer size value
     */
    public static int defaultBufferSize() {
        return DEFAULT_BUFFER_SIZE;
    }

}