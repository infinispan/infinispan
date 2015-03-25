package org.infinispan.client.hotrod.impl.protocol;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.transport.Transport;

public class Codec22 extends Codec21 {
   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_22);
   }

   @Override
   public void writeExpirationParams(Transport transport, long lifespanNanos, long maxIdleNanos, InternalFlag[] internalFlags) {
      if (!hasFlag(internalFlags, InternalFlag.NANO_DURATIONS)) {
         lifespanNanos = TimeUnit.SECONDS.convert(lifespanNanos, TimeUnit.NANOSECONDS);
         maxIdleNanos = TimeUnit.SECONDS.convert(maxIdleNanos, TimeUnit.NANOSECONDS);
      }
      transport.writeVLong(lifespanNanos);
      transport.writeVLong(maxIdleNanos);
   }

   private boolean hasFlag(InternalFlag[] internalFlags, InternalFlag flag) {
      if (internalFlags == null) {
         return false;
      }
      for (InternalFlag internalFlag : internalFlags) {
         if (internalFlag == flag) {
            return true;
         }
      }
      return false;
   }
}
