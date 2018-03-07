package org.infinispan.stream.impl;

import java.util.Collection;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.CacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Manages distribution of various stream operations that are sent to remote nodes.  Note usage of any operations
 * should <b>always</b> be accompanied with a subsequent call to {@link ClusterStreamManager#forgetOperation(Object)}
 * so that the operation is fully released.  This is important especially for early terminating operations.
 * @param <K> The key type for the underlying cache
 */
public interface ClusterStreamManager<Original, K> {
   /**
    * A callback that is used for result processing from the remote nodes.
    * @param <R> the type of results returned
    */
   interface ResultsCallback<R> {
      /**
       * Called back for intermediate data returned from an operation.  This is useful for operations that utilized
       * batch fetching such as {@link CacheStream#iterator()}, {@link CacheStream#spliterator()},
       * {@link CacheStream#forEach(Consumer)} and {@link CacheStream#toArray()}.
       * @param address Which node this data came from
       * @param results The results obtained so far.
       * @return the segments that completed with some value
       */
      Set<Integer> onIntermediateResult(Address address, R results);

      /**
       * Essentially the same as {@link ClusterStreamManager.ResultsCallback#onIntermediateResult(Address address, Object)}
       * except that this is the last time this callback will be invoked and it tells which segments were completed
       * @param address Which node this data came from
       * @param results The last batch of results for this operator
       */
      void onCompletion(Address address, Set<Integer> completedSegments, R results);

      /**
       * Called back when a segment is found to have been lost that is no longer remote
       * This method should return as soon as possible and not block in any fashion.
       * This method may be invoked concurrently with any of the other methods
       * @param segments The segments that were requested but are now local
       */
      void onSegmentsLost(Set<Integer> segments);

      /**
       * Called when a an owner of a segment is not available in the provided {@link ConsistentHash}
       */
      void requestFutureTopology();
   }

   /**
    * Performs the remote stream operation without rehash awareness.
    * @param <R> the type of response
    * @param parallelDistribution whether or not parallel distribution is enabled
    * @param parallelStream whether or not the stream is paralllel
    * @param ch the consistent hash to use when determining segment ownership
    * @param segments the segments that this request should utilize
    * @param keysToInclude which keys to include in the request
    * @param keysToExclude which keys to exclude in the request
    * @param includeLoader whether or not to use a loader
    * @param entryStream whether the remote stream should be an entry or key stream
    * @param operation the actual operation to perform
    * @param callback the callback to collect individual node results
    * @param earlyTerminatePredicate a predicate to determine if this operation should stop based on intermediate results
    * @return the operation id to be used for further calls
    */
   <R> Object remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
         Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
         boolean entryStream, TerminalOperation<Original, R> operation, ResultsCallback<R> callback,
         Predicate<? super R> earlyTerminatePredicate);

   /**
    * Performs the remote stream operation with rehash awareness.
    * @param <R> the type of response
    * @param parallelDistribution whether or not parallel distribution is enabled
    * @param parallelStream whether or not the stream is paralllel
    * @param ch the consistent hash to use when determining segment ownership
    * @param segments the segments that this request should utilize
    * @param keysToInclude which keys to include in the request
    * @param keysToExclude which keys to exclude in the request
    * @param includeLoader whether or not to use a loader
    * @param entryStream whether the remote stream should be an entry or key stream
    * @param operation the actual operation to perform
    * @param callback the callback to collect individual node results
    * @param earlyTerminatePredicate a predicate to determine if this operation should stop based on intermediate results
    * @return the operation id to be used for further calls
    */
   <R> Object remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
         Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
         boolean entryStream, TerminalOperation<Original, R> operation, ResultsCallback<R> callback,
         Predicate<? super R> earlyTerminatePredicate);

   /**
    * Key tracking remote operation that doesn't have rehash enabled.
    * @param <R> the type of response
    * @param parallelDistribution whether or not parallel distribution is enabled
    * @param parallelStream whether or not the stream is paralllel
    * @param ch the consistent hash to use when determining segment ownership
    * @param segments the segments that this request should utilize
    * @param keysToInclude which keys to include in the request
    * @param keysToExclude which keys to exclude in the request
    * @param includeLoader whether or not to use a loader
    * @param entryStream whether the remote stream should be an entry or key stream
    * @param operation the actual operation to perform
    * @param callback the callback to collect individual node results
    * @return the operation id to be used for further calls
    */
   <R> Object remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
         Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
         boolean entryStream, KeyTrackingTerminalOperation<Original, K, R> operation,
         ResultsCallback<Collection<R>> callback);

   /**
    * Key tracking remote operation that has rehash enabled
    * @param parallelDistribution whether or not parallel distribution is enabled
    * @param parallelStream whether or not the stream is paralllel
    * @param ch the consistent hash to use when determining segment ownership
    * @param segments the segments that this request should utilize
    * @param keysToInclude which keys to include in the request
    * @param keysToExclude which keys to exclude in the request
    * @param includeLoader whether or not to use a loader
    * @param entryStream whether the remote stream should be an entry or key stream
    * @param operation the actual operation to perform
    * @param callback the callback to collect individual node results
    * @return the operation id to be used for further calls
    */
   Object remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
         Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
         boolean entryStream, KeyTrackingTerminalOperation<Original, K, ?> operation, ResultsCallback<Collection<K>> callback);

   /**
    * Tests whether this operation is still pending or not.
    * @param id the id of the operation that was returned from the invocation
    * @return whether or not it is completed
    */
   boolean isComplete(Object id);

   /**
    * Awaits completion of the given request.  Returns true when the request completes otherwise returns false after
    * the time elapses
    * @param id the ide of the operation that was returned from the invocation - must be non null
    * @param time how long to wait before returning false - must be greater than 0
    * @param unit controls how long the time wait is
    * @return whether or not the request is complete
    */
   boolean awaitCompletion(Object id, long time, TimeUnit unit) throws InterruptedException;

   /**
    * Frees any resources related to this operation and signals to any ongoing remote operations to no longer continue
    * processing
    * @param id the ide of the operation that was returned from the invocation - can be null in which case this is a noop
    */
   void forgetOperation(Object id);

   /**
    * Receives a response for a given request
    * @param id The request id
    * @param origin The origin of the response
    * @param complete Whether or not this is a completed response
    * @param segments The segments that were suspected
    * @param response The actual response value
    * @param <R1> The type of the response
    * @return Whether or not the operation should continue operating, only valid if complete was false
    */
   <R1> boolean receiveResponse(Object id, Address origin, boolean complete, Set<Integer> segments, R1 response);

   /**
    *
    * @param parallelStream
    * @param segments
    * @param keysToInclude
    * @param keysToExclude
    * @param includeLoader
    * @param entryStream whether the remote stream should be an entry or key stream
    * @param intermediateOperations
    * @param <E>
    * @return
    */
   <E> RemoteIteratorPublisher<E> remoteIterationPublisher(boolean parallelStream,
         Supplier<Map.Entry<Address, IntSet>> segments, Set<K> keysToInclude, IntFunction<Set<K>> keysToExclude,
         boolean includeLoader, boolean entryStream, Iterable<IntermediateOperation> intermediateOperations);

   /**
    * {@inheritDoc}
    * <p>
    * This producer is used to allow for it to provide additional signals to the subscribing caller. Callers may want
    * to use the producer as a standard producer but also listen for the additional lost segment signal. This can
    * be accomplished by doing the following:
    * <pre>{@code Publisher<K> publisher = s -> remoteIteratorPublisher.subscribe(s, segments -> { // Do something};}</pre>
    */
   interface RemoteIteratorPublisher<K> extends Publisher<K> {
      /**
       * Essentially the same as {@link Publisher#subscribe(Subscriber)} except that a {@link Consumer} is provided
       * that will be invoked when a segment for a given request has been lost. This is to notify the subscribing
       * code that they may have to rerequest such data.
       * <p>
       * This publisher guarantees it will call the {@link Consumer}s before {@link Subscriber#onComplete()} and it will
       * not call them concurrently. The provided segments will always be non null, however it could be empty. However
       * it is possible that a key returned could be mapped to a segment that was found to be suspected or completed.
       * @see Publisher#subscribe(Subscriber)
       * @param s the subscriber to subscribe
       * @param completedSegments the consumer to  be notified of completed segments
       * @param lostSegments the consumer to be notified of lost segments
       */
      void subscribe(Subscriber<? super K> s, Consumer<? super Supplier<PrimitiveIterator.OfInt>> completedSegments,
            Consumer<? super Supplier<PrimitiveIterator.OfInt>> lostSegments);

      @Override
      default void subscribe(Subscriber<? super K> s) {
         subscribe(s, completedSegments -> { }, lostSegments -> { });
      }
   }
}
