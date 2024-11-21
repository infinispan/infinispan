package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;

import io.netty.buffer.ByteBuf;

/**
 * @since 15.1
 */
public class Codec41 extends Codec40 {
   @Override
   public void writeHeader(ByteBuf buf, long messageId, ClientTopology clientTopology, HotRodOperation<?> operation) {
      writeHeader(buf, messageId, clientTopology, operation, HotRodConstants.VERSION_41);
   }
}
