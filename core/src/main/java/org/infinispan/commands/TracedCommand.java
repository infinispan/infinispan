package org.infinispan.commands;

import org.infinispan.telemetry.InfinispanSpanAttributes;

public interface TracedCommand {

   default InfinispanSpanAttributes getSpanAttributes() {
      return null;
   }

   default String getOperationName() {
      if (getSpanAttributes() != null) {
         throw new IllegalStateException("getOperationName() must be implemented when getSpanAttributes() is.");
      }
      return null;
   }

   default void setSpanAttributes(InfinispanSpanAttributes attributes) {
      //no-op
   }

}
