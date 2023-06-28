package org.infinispan.telemetry;

import java.util.Locale;

public enum SpanCategory {

   /**
    * Default Infinispan span category, which includes all the major put/insertion operations.
    */
   CONTAINER;

   @Override
   public String toString() {
      return name().toLowerCase(Locale.ROOT);
   }
}
