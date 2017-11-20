package org.infinispan.server.hotrod.counter.impl;

import static org.infinispan.server.hotrod.counter.listener.ClientCounterEvent.decodeNewState;
import static org.infinispan.server.hotrod.counter.listener.ClientCounterEvent.decodeOldState;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterState;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;

/**
 * The test counter event.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class TestCounterEventResponse extends TestResponse {

   private final String counterName;
   private final WrappedByteArray listenerId;
   private final CounterEvent counterEvent;

   public TestCounterEventResponse(byte version, long messageId, HotRodOperation operation, ByteBuf buffer) {
      super(version, messageId, "", (byte) 0, operation, OperationStatus.Success, 0, null);
      counterName = ExtendedByteBuf.readString(buffer);
      listenerId = new WrappedByteArray(ExtendedByteBuf.readRangedBytes(buffer));
      byte counterState = buffer.readByte();
      counterEvent = new CounterEventImpl(buffer.readLong(), decodeOldState(counterState), buffer.readLong(),
            decodeNewState(counterState));

   }


   public String getCounterName() {
      return counterName;
   }

   public WrappedByteArray getListenerId() {
      return listenerId;
   }

   CounterEvent getCounterEvent() {
      return counterEvent;
   }

   private static class CounterEventImpl implements CounterEvent {

      private final long oldValue;
      private final CounterState oldState;
      private final long newValue;
      private final CounterState newState;

      private CounterEventImpl(long oldValue, CounterState oldState, long newValue,
            CounterState newState) {
         this.oldValue = oldValue;
         this.oldState = oldState;
         this.newValue = newValue;
         this.newState = newState;
      }

      @Override
      public long getOldValue() {
         return oldValue;
      }

      @Override
      public CounterState getOldState() {
         return oldState;
      }

      @Override
      public long getNewValue() {
         return newValue;
      }

      @Override
      public CounterState getNewState() {
         return newState;
      }
   }
}
