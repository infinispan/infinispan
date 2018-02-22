package org.infinispan.server.hotrod.counter.listener;

import org.infinispan.commons.util.Util;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterState;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;

/**
 * A counter event to send to a client.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class ClientCounterEvent {

   private final String counterName;
   private final CounterEvent event;
   private final byte version;
   private final byte[] listenerId;

   ClientCounterEvent(byte[] listenerId, byte version, String counterName, CounterEvent event) {
      this.version = version;
      this.counterName = counterName;
      this.event = event;
      this.listenerId = listenerId;
   }

   public static CounterState decodeOldState(byte encoded) {
      switch (encoded & 0x03) {
         case 0:
            return CounterState.VALID;
         case 0x01:
            return CounterState.LOWER_BOUND_REACHED;
         case 0x02:
            return CounterState.UPPER_BOUND_REACHED;
         default:
            throw new IllegalStateException();
      }
   }

   public static CounterState decodeNewState(byte encoded) {
      switch (encoded & 0x0C) {
         case 0:
            return CounterState.VALID;
         case 0x04:
            return CounterState.LOWER_BOUND_REACHED;
         case 0x08:
            return CounterState.UPPER_BOUND_REACHED;
         default:
            throw new IllegalStateException();
      }
   }

   public byte getVersion() {
      return version;
   }

   public void writeTo(ByteBuf buffer) {
      ExtendedByteBuf.writeString(counterName, buffer);
      ExtendedByteBuf.writeRangedBytes(listenerId, buffer);
      buffer.writeByte(encodeCounterStates());
      buffer.writeLong(event.getOldValue());
      buffer.writeLong(event.getNewValue());
   }

   @Override
   public String toString() {
      return "ClientCounterEvent{" +
             "counterName='" + counterName + '\'' +
             ", event=" + event +
             ", version=" + version +
             ", listenerId=" + Util.printArray(listenerId) +
             '}';
   }

   /*
      ----|00|00
           |  ^ old state: 0 = valid, 1 = lower, 2 = upper
           ^----new state: 0 = valid, 4 = lower, 8 = upper
       */
   private int encodeCounterStates() {
      byte encoded = 0;
      switch (event.getOldState()) {
         case LOWER_BOUND_REACHED:
            encoded = 0x01;
            break;
         case UPPER_BOUND_REACHED:
            encoded = 0x02;
            break;
         case VALID:
         default:
            break;
      }
      switch (event.getNewState()) {
         case LOWER_BOUND_REACHED:
            encoded |= 0x04;
            break;
         case UPPER_BOUND_REACHED:
            encoded |= 0x08;
            break;
         case VALID:
         default:
            break;
      }
      return encoded;
   }
}
