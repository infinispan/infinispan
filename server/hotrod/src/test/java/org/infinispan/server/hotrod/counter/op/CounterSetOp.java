package org.infinispan.server.hotrod.counter.op;

import static org.infinispan.server.hotrod.HotRodOperation.COUNTER_GET_AND_SET;

import io.netty.buffer.ByteBuf;

public class CounterSetOp extends CounterOp {

   private final long value;

   public CounterSetOp(byte version, String counterName, long value) {
      super(version, COUNTER_GET_AND_SET, counterName);
      this.value = value;
   }

   @Override
   public void writeTo(ByteBuf buffer) {
      super.writeTo(buffer);
      buffer.writeLong(value);
   }
}
