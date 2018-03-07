package org.infinispan.stream.impl;

import java.util.Set;

import org.infinispan.commons.util.IntSet;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Stream manager that is invoked on a local node.  This is normally called due to a {@link ClusterStreamManager} from
 * another node requiring some operation to be performed
 * @param <Original> original stream type
 * @param <K> the key type for the operations
 */
public interface LocalStreamManager<Original, K> {
   /**
    * Stream operation for a non key aware operation without rehash enabled.
    * @param requestId the originating request id
    * @param origin the node this request came from
    * @param parallelStream whether this stream is parallel or not
    * @param segments the segments to include in this operation
    * @param keysToInclude which keys to include
    * @param keysToExclude which keys to exclude
    * @param includeLoader whether or not a cache loader should be utilized
    * @param operation the operation to perform
    * @param <R> the type of value from the operation
    */
   <R> void streamOperation(Object requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
         TerminalOperation<Original, R> operation);

   /**
    * Stream operation for a non key aware operation with rehash enabled.
    * @param requestId the originating request id
    * @param origin the node this request came from
    * @param parallelStream whether this stream is parallel or not
    * @param segments the segments to include in this operation
    * @param keysToInclude which keys to include
    * @param keysToExclude which keys to exclude
    * @param includeLoader whether or not a cache loader should be utilized
    * @param operation the operation to perform
    * @param <R> the type of value from the operation
    */
   <R> void streamOperationRehashAware(Object requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
         TerminalOperation<Original, R> operation);

   /**
    * Stream operation for a key aware operation without rehash enabled
    * @param requestId the originating request id
    * @param origin the node this request came from
    * @param parallelStream whether this stream is parallel or not
    * @param segments the segments to include in this operation
    * @param keysToInclude which keys to include
    * @param keysToExclude which keys to exclude
    * @param includeLoader whether or not a cache loader should be utilized
    * @param operation the operation to perform
    * @param <R> the type of value from the operation
    */
   <R> void streamOperation(Object requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
           KeyTrackingTerminalOperation<Original, K, R> operation);

   /**
    * Stream operation for a key aware operation with rehash enabled
    * @param requestId the originating request id
    * @param origin the node this request came from
    * @param parallelStream whether this stream is parallel or not
    * @param segments the segments to include in this operation
    * @param keysToInclude which keys to include
    * @param keysToExclude which keys to exclude
    * @param includeLoader whether or not a cache loader should be utilized
    * @param operation the operation to perform
    */
   void streamOperationRehashAware(Object requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
           KeyTrackingTerminalOperation<Original, K, ?> operation);

   /**
    * Signals that a new iterator is created using the given arguments. Returns a response which only returns the given
    * <b>batchSize</b> worth of elements.
    * @param requestId the originating request id
    * @param origin the node this request came from
    * @param segments the segments to include in this operation
    * @param keysToInclude which keys to include
    * @param keysToExclude which keys to exclude
    * @param includeLoader whether or not a cache loader should be utilized
    * @param intermediateOperations the operations to apply to the underlying data
    * @param batchSize how many elements to return
    * @return the response containing iterator
    */
   IteratorResponse startIterator(Object requestId, Address origin, IntSet segments, Set<K> keysToInclude,
         Set<K> keysToExclude, boolean includeLoader, boolean entryStream,
         Iterable<IntermediateOperation> intermediateOperations, long batchSize);

   /**
    * Continues an existing iterator by retrieving the next <b>batchSize</b> of elements
    * @param requestId the originating request id
    * @param batchSize how many elements to return
    * @return the response containing iterator
    */
   IteratorResponse continueIterator(Object requestId, long batchSize);
}
