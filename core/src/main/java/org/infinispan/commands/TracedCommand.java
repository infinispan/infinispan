package org.infinispan.commands;

import org.infinispan.telemetry.InfinispanSpanAttributes;

public interface TracedCommand {

   default InfinispanSpanAttributes getSpanAttributes() {
      return null;
   }

   default String getOperationName() {
      return getClass().getSimpleName();
   }

   default void setSpanAttributes(InfinispanSpanAttributes attributes) {
      //no-op
   }
}
