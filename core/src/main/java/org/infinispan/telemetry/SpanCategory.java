package org.infinispan.telemetry;

import java.util.Locale;

public enum SpanCategory {

   /**
    * Default Infinispan span category, which includes all the major put/insertion operations.
    */
   CONTAINER,

   /**
    * Span category for cluster operations causally related by client interactions.
    */
   CLUSTER,

   /**
    * Span category for x-site operations causally related by client interactions.
    */
   X_SITE;

   @Override
   public String toString() {
      return name().toLowerCase(Locale.ROOT);
   }
}
