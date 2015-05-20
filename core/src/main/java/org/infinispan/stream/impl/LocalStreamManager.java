package org.infinispan.stream.impl;

import org.infinispan.remoting.transport.Address;

import java.util.Set;
import java.util.UUID;

/**
 * Stream manager that is invoked on a local node.  This is normally called due to a {@link ClusterStreamManager} from
 * another node requiring some operation to be performed
 * @param <K> the key type for the operations
 */
public interface LocalStreamManager<K> {
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
   <R> void streamOperation(UUID requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, TerminalOperation<R> operation);

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
   <R> void streamOperationRehashAware(UUID requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader, TerminalOperation<R> operation);

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
   <R> void streamOperation(UUID requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, R, ?> operation);

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
    * @param <R2> the type of response
    */
   <R2> void streamOperationRehashAware(UUID requestId, Address origin, boolean parallelStream, Set<Integer> segments,
           Set<K> keysToInclude, Set<K> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, ?, R2> operation);
}
