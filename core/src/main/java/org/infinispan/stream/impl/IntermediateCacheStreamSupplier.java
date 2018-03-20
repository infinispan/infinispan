package org.infinispan.stream.impl;

import java.util.Set;
import java.util.stream.BaseStream;

import org.infinispan.BaseCacheStream;
import org.infinispan.stream.impl.local.AbstractLocalCacheStream;

/**
 * Stream supplier that is used when a local intermediate operation is invoked, requiring a combined remote and local
 * operation stream.
 */
class IntermediateCacheStreamSupplier<T, S extends BaseStream<T, S>> implements AbstractLocalCacheStream.StreamSupplier<T, S> {
   final IntermediateType type;
   final BaseCacheStream streamable;

   IntermediateCacheStreamSupplier(IntermediateType type, BaseCacheStream streamable) {
      this.type = type;
      this.streamable = streamable;
   }


   @Override
   public S buildStream(Set<Integer> segmentsToFilter, Set<?> keysToFilter) {
      return (S) type.handleStream(streamable);
   }
}
