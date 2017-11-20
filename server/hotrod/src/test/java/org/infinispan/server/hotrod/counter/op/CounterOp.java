package org.infinispan.server.hotrod.counter.op;

import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeRangedBytes;

import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.test.Op;

import io.netty.buffer.ByteBuf;

/**
 * A base counter test operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterOp extends Op {

   private final String counterName;

   public CounterOp(byte version, HotRodOperation operation, String counterName) {
      super(0xA0, version, (byte) operation.getRequestOpCode(), "", null, 0, 0, null, 0, 0, (byte) 0, 0);
      this.counterName = counterName;
   }

   public void writeTo(ByteBuf buffer) {
      writeRangedBytes(counterName.getBytes(), buffer);
   }
}
