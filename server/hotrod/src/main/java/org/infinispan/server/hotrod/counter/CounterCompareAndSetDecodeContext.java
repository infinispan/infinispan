package org.infinispan.server.hotrod.counter;

import java.util.Optional;

import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;

/**
 * A decode context for {@link HotRodOperation#COUNTER_CAS} operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterCompareAndSetDecodeContext extends CounterDecodeContext {

   private static final Log log = LogFactory.getLog(CounterAddDecodeContext.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private long expected;
   private long update;
   private DecodeState decodeState = DecodeState.DECODE_EXPECTED;

   public long getExpected() {
      return expected;
   }

   public long getUpdate() {
      return update;
   }

   @Override
   DecodeStep nextStep() {
      switch (decodeState) {
         case DECODE_UPDATE:
            return this::decodeUpdate;
         case DECODE_DONE:
            return null;
         case DECODE_EXPECTED:
            return this::decodeExpected;
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   boolean trace() {
      return trace;
   }

   @Override
   Log log() {
      return log;
   }

   private boolean decodeExpected(ByteBuf buffer) {
      Optional<Long> optValue = ExtendedByteBuf.readMaybeLong(buffer);
      optValue.ifPresent(value -> {
         this.expected = value;
         logDecoded("expected-value", value);
         decodeState = DecodeState.DECODE_UPDATE;
      });
      return !optValue.isPresent();
   }

   private boolean decodeUpdate(ByteBuf buffer) {
      Optional<Long> optValue = ExtendedByteBuf.readMaybeLong(buffer);
      optValue.ifPresent(value -> {
         this.update = value;
         logDecoded("update-value", value);
         decodeState = DecodeState.DECODE_DONE;
      });
      return !optValue.isPresent();
   }

   private enum DecodeState {
      DECODE_EXPECTED,
      DECODE_UPDATE,
      DECODE_DONE
   }

}
