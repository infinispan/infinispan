package org.infinispan.server.hotrod.counter;

import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeString;

import java.util.Optional;

import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;

/**
 * A base decode context for counter's Hot Rod operations.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public abstract class CounterDecodeContext {


   private String counterName;
   private DecodeStep nextDecodeStep = this::decodeName;

   public String getCounterName() {
      return counterName;
   }

   /**
    * @return {@code true} if decode is completed.
    */
   public final boolean decode(ByteBuf buffer) {
      while (nextDecodeStep != null) {
         if (nextDecodeStep.decode(buffer)) {
            return false;
         }
         buffer.markReaderIndex();
         nextDecodeStep = nextStep();
      }
      return true;
   }

   abstract DecodeStep nextStep();

   final void logDecoded(String name, Object value) {
      if (trace()) {
         log().tracef("Decode '%s'='%s'", name, value);
      }
   }

   abstract boolean trace();

   abstract Log log();

   private boolean decodeName(ByteBuf buffer) {
      Optional<String> optName = readMaybeString(buffer);
      optName.ifPresent(name -> {
         counterName = name;
         logDecoded("counter name", name);
      });
      return !optName.isPresent();
   }

   interface DecodeStep {
      /**
       * @return {@code true} if needs more data and it isn't available.
       */
      boolean decode(ByteBuf buffer);
   }


}
