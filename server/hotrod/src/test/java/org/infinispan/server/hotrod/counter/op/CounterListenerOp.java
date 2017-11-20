package org.infinispan.server.hotrod.counter.op;

import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;

/**
 * Test operation for {@link HotRodOperation#COUNTER_ADD_LISTENER} and {@link HotRodOperation#COUNTER_REMOVE_LISTENER}
 * operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterListenerOp extends CounterOp {

   private final byte[] listenerId;

   private CounterListenerOp(byte version, HotRodOperation operation,
         String counterName, byte[] listenerId) {
      super(version, operation, counterName);
      this.listenerId = listenerId;
   }

   public static CounterListenerOp createListener(byte version, String counterName, byte[] id) {
      return new CounterListenerOp(version, HotRodOperation.COUNTER_ADD_LISTENER, counterName, id);
   }

   public static CounterListenerOp removeListener(byte version, String counterName, byte[] id) {
      return new CounterListenerOp(version, HotRodOperation.COUNTER_REMOVE_LISTENER, counterName, id);
   }

   @Override
   public void writeTo(ByteBuf buffer) {
      super.writeTo(buffer);
      ExtendedByteBuf.writeRangedBytes(listenerId, buffer);
   }
}
