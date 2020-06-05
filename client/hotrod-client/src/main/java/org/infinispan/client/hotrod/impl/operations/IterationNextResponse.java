package org.infinispan.client.hotrod.impl.operations;

import java.util.List;
import java.util.Map.Entry;

import org.infinispan.commons.util.IntSet;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextResponse<E> {
   private final short status;
   private final List<Entry<Object, E>> entries;
   private final IntSet completedSegments;

   private final boolean hasMore;

   public IterationNextResponse(short status, List<Entry<Object, E>> entries, IntSet completedSegments, boolean hasMore) {
      this.status = status;
      this.entries = entries;
      this.completedSegments = completedSegments;
      this.hasMore = hasMore;
   }

   public boolean hasMore() {
      return hasMore;
   }

   public List<Entry<Object, E>> getEntries() {
      return entries;
   }

   public short getStatus() {
      return status;
   }

   public IntSet getCompletedSegments() {
      return completedSegments;
   }
}
