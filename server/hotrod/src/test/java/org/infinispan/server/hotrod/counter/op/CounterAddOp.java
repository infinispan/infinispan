package org.infinispan.server.hotrod.counter.op;

import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_ADD_AND_GET;

import org.infinispan.server.hotrod.HotRodOperation;

import io.netty.buffer.ByteBuf;

/**
 * Test operation for {@link HotRodOperation#COUNTER_ADD_AND_GET} operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterAddOp extends CounterOp {

   private final long value;

   public CounterAddOp(byte version, String counterName, long value) {
      super(version, COUNTER_ADD_AND_GET, counterName);
      this.value = value;
   }

   @Override
   public void writeTo(ByteBuf buffer) {
      super.writeTo(buffer);
      buffer.writeLong(value);
   }
}
