package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextResponse<K, V> {
   private final short status;
   private final byte[] finishedSegments;
   private final Map.Entry<byte[], Object[]>[] entries;

   public IterationNextResponse(short status, byte[] finishedSegments, Map.Entry<byte[], Object[]>[] entries) {
      this.status = status;
      this.finishedSegments = finishedSegments;
      this.entries = entries;
   }

   public byte[] getFinishedSegments() {
      return finishedSegments;
   }

   public Map.Entry<byte[], Object[]>[] getEntries() {
      return entries;
   }

   public short getStatus() {
      return status;
   }
}
