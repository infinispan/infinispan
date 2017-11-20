package org.infinispan.server.hotrod.counter.op;

import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_CAS;

import org.infinispan.server.hotrod.HotRodOperation;

import io.netty.buffer.ByteBuf;

/**
 * A test operation for {@link HotRodOperation#COUNTER_CAS} operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterCompareAndSwapOp extends CounterOp {

   private final long expected;
   private final long update;

   public CounterCompareAndSwapOp(byte version, String counterName, long expected, long update) {
      super(version, COUNTER_CAS, counterName);
      this.expected = expected;
      this.update = update;
   }

   @Override
   public void writeTo(ByteBuf buffer) {
      super.writeTo(buffer);
      buffer.writeLong(expected);
      buffer.writeLong(update);
   }
}
