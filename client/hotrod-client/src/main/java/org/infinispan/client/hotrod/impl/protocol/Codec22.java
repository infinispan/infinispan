package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.transport.Transport;

public class Codec22 extends Codec21 {
   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_22);
   }

   @Override
   public void writeExpirationNanoTimes(Transport transport, int lifespanNanos, int maxIdleNanos, InternalFlag[] internalFlags) {
      if (hasFlag(internalFlags, InternalFlag.LIFESPAN_NANOS)) {
         transport.writeVInt(lifespanNanos);
      }
      if (hasFlag(internalFlags, InternalFlag.MAXIDLE_NANOS)) {
         transport.writeVInt(maxIdleNanos);
      }
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
