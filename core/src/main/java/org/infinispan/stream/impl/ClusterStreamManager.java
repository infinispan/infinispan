package org.infinispan.stream.impl;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Manages distribution of various stream operations that are sent to remote nodes.  Note usage of any operations
 * should <b>always</b> be accompanied with a subsequent call to {@link ClusterStreamManager#forgetOperation(UUID)}
 * so that the operation is fully released.  This is important especially for early terminating operations.
 * @param <K> The key type for the underlying cache
 */
public interface ClusterStreamManager<K> {
   /**
    * A callback that is used for result processing from the remote nodes.
    * @param <R> the type of results returned
    */
   interface ResultsCallback<R> {
      /**
       * Called back for intermediate data returned from an operation.  This is useful
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
   }

   /**
    * Performs the remote stream operation without rehash awareness.
    * @param parallelDistribution whether or not parallel distribution is enabled
    * @param parallelStream whether or not the stream is paralllel
    * @param ch the consistent hash to use when determining segment ownership
    * @param segments the segments that this request should utilize
    * @param keysToInclude which keys to include in the request
    * @param keysToExclude which keys to exclude in the request
    * @param includeLoader whether or not to use a loader
    * @param operation the actual operation to perform
    * @param callback the callback to collect individual node results
    * @param earlyTerminatePredicate a predicate to determine if this operation should stop based on intermediate results
    * @param <R> the type of response
    * @return the operation id to be used for further calls
    */
   <R> UUID remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           TerminalOperation<R> operation, ResultsCallback<R> callback, Predicate<? super R> earlyTerminatePredicate);

   /**
    * Performs the remote stream operation with rehash awareness.
    * @param parallelDistribution whether or not parallel distribution is enabled
    * @param parallelStream whether or not the stream is paralllel
    * @param ch the consistent hash to use when determining segment ownership
    * @param segments the segments that this request should utilize
    * @param keysToInclude which keys to include in the request
    * @param keysToExclude which keys to exclude in the request
    * @param includeLoader whether or not to use a loader
    * @param operation the actual operation to perform
    * @param callback the callback to collect individual node results
    * @param earlyTerminatePredicate a predicate to determine if this operation should stop based on intermediate results
    * @param <R> the type of response
    * @return the operation id to be used for further calls
    */
   <R> UUID remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           TerminalOperation<R> operation, ResultsCallback<R> callback, Predicate<? super R> earlyTerminatePredicate);

   /**
    * Key tracking remote operation that doesn't have rehash enabled.
    * @param parallelDistribution whether or not parallel distribution is enabled
    * @param parallelStream whether or not the stream is paralllel
    * @param ch the consistent hash to use when determining segment ownership
    * @param segments the segments that this request should utilize
    * @param keysToInclude which keys to include in the request
    * @param keysToExclude which keys to exclude in the request
    * @param includeLoader whether or not to use a loader
    * @param operation the actual operation to perform
    * @param callback the callback to collect individual node results
    * @param <R> the type of response
    * @return the operation id to be used for further calls
    */
   <R> UUID remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, R, ?> operation, ResultsCallback<Collection<R>> callback);

   /**
    * Key tracking remote operation that has rehash enabled
    * @param parallelDistribution whether or not parallel distribution is enabled
    * @param parallelStream whether or not the stream is paralllel
    * @param ch the consistent hash to use when determining segment ownership
    * @param segments the segments that this request should utilize
    * @param keysToInclude which keys to include in the request
    * @param keysToExclude which keys to exclude in the request
    * @param includeLoader whether or not to use a loader
    * @param operation the actual operation to perform
    * @param callback the callback to collect individual node results
    * @param <R2> the type of response
    * @return the operation id to be used for further calls
    */
   <R2> UUID remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
                                                 Set<Integer> segments, Set<K> keysToInclude,
                                                 Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
                                                 KeyTrackingTerminalOperation<K, ?, R2> operation,
                                                 ResultsCallback<Map<K, R2>> callback);

   /**
    * Tests whether this operation is still pending or not.
    * @param id the id of the operation that was returned from the invocation
    * @return whether or not it is completed
    */
   boolean isComplete(UUID id);

   /**
    * Awaits completion of the given request.  Returns true when the request completes otherwise returns false after
    * the time elapses
    * @param id the ide of the operation that was returned from the invocation
    * @param time how long to wait before returning false
    * @param unit controls how long the time wait is
    * @return whether or not the request is complete
    */
   boolean awaitCompletion(UUID id, long time, TimeUnit unit) throws InterruptedException;

   /**
    * Frees any resources related to this operation and signals to any ongoing remote operations to no longer continue
    * processing
    * @param id the ide of the operation that was returned from the invocation
    */
   void forgetOperation(UUID id);

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
   <R1> boolean receiveResponse(UUID id, Address origin, boolean complete, Set<Integer> segments, R1 response);
}
