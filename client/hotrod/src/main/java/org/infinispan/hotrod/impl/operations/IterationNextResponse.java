package org.infinispan.hotrod.impl.operations;

import java.util.List;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.commons.util.IntSet;

/**
 * @since 14.0
 */
public class IterationNextResponse<K, E> {
   private final short status;
   private final List<CacheEntry<K, E>> entries;
   private final IntSet completedSegments;

   private final boolean hasMore;

   public IterationNextResponse(short status, List<CacheEntry<K, E>> entries, IntSet completedSegments, boolean hasMore) {
      this.status = status;
      this.entries = entries;
      this.completedSegments = completedSegments;
      this.hasMore = hasMore;
   }

   public boolean hasMore() {
      return hasMore;
   }

   public List<CacheEntry<K, E>> getEntries() {
      return entries;
   }

   public short getStatus() {
      return status;
   }

   public IntSet getCompletedSegments() {
      return completedSegments;
   }
}
