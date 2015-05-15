package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextResponse {
   private final short status;
   private final byte[] finishedSegments;
   private final Map.Entry<byte[], byte[]>[] entries;

   public IterationNextResponse(short status, byte[] finishedSegments, Map.Entry<byte[], byte[]>[] entries) {
      this.status = status;
      this.finishedSegments = finishedSegments;
      this.entries = entries;
   }

   public byte[] getFinishedSegments() {
      return finishedSegments;
   }

   public Map.Entry<byte[], byte[]>[] getEntries() {
      return entries;
   }

   public short getStatus() {
      return status;
   }
}
