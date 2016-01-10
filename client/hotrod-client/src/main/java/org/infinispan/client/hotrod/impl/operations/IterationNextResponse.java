package org.infinispan.client.hotrod.impl.operations;

import java.util.List;
import java.util.Map.Entry;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextResponse<E> {
   private final short status;
   private final List<Entry<Object, E>> entries;

   private final boolean hasMore;

   public IterationNextResponse(short status, List<Entry<Object, E>> entries, boolean hasMore) {
      this.status = status;
      this.entries = entries;
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
}
