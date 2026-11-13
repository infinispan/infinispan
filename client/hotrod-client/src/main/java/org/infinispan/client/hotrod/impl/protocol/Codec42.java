package org.infinispan.client.hotrod.impl.protocol;

import java.util.BitSet;

import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;

import io.netty.buffer.ByteBuf;

/**
 * @since 16.4
 */
public class Codec42 extends Codec41 {
   public Codec42(BitSet supportedOps) {
      super(supportedOps);
   }

   @Override
   public void writeHeader(ByteBuf buf, long messageId, ClientTopology clientTopology, HotRodOperation<?> operation) {
      writeHeader(buf, messageId, clientTopology, operation, HotRodConstants.VERSION_42);
   }

   @Override
   public boolean isPrimaryNearCacheListenerSupported() {
      return true;
   }
}
