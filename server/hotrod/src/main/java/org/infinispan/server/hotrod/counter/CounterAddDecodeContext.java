package org.infinispan.server.hotrod.counter;

import java.util.Optional;

import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;

/**
 * A decode context for {@link HotRodOperation#COUNTER_ADD_AND_GET} operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterAddDecodeContext extends CounterDecodeContext {
   private static final Log log = LogFactory.getLog(CounterAddDecodeContext.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private long value;
   private boolean decodeDone = false;

   public long getValue() {
      return value;
   }

   @Override
   DecodeStep nextStep() {
      return decodeDone ? null : this::decodeValue;
   }

   @Override
   boolean trace() {
      return trace;
   }

   @Override
   Log log() {
      return log;
   }

   private boolean decodeValue(ByteBuf buffer) {
      Optional<Long> optValue = ExtendedByteBuf.readMaybeLong(buffer);
      optValue.ifPresent(value -> {
         this.value = value;
         logDecoded("value", value);
         decodeDone = true;
      });
      return !optValue.isPresent();
   }
}
