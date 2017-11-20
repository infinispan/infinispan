package org.infinispan.server.hotrod.counter;

import java.util.Optional;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;

/**
 * A decode context for {@link HotRodOperation#COUNTER_ADD_LISTENER} and {@link HotRodOperation#COUNTER_REMOVE_LISTENER}
 * operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterListenerDecodeContext extends CounterDecodeContext {

   private static final Log log = LogFactory.getLog(CounterListenerDecodeContext.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private byte[] listenerId;
   private boolean done = false;

   public byte[] getListenerId() {
      return listenerId;
   }

   @Override
   DecodeStep nextStep() {
      return done ? null : this::decodeListenerId;
   }

   @Override
   boolean trace() {
      return trace;
   }

   @Override
   Log log() {
      return log;
   }


   private boolean decodeListenerId(ByteBuf buffer) {
      Optional<byte[]> optValue = ExtendedByteBuf.readMaybeRangedBytes(buffer);
      optValue.ifPresent(value -> {
         this.listenerId = value;
         logDecoded("listener-id", Util.printArray(value));
         done = true;
      });
      return !optValue.isPresent();
   }

}
