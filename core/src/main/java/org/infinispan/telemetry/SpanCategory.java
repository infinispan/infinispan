package org.infinispan.telemetry;

import static java.util.Arrays.stream;

import org.infinispan.commons.configuration.io.NamingStrategy;

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
   X_SITE,

   /**
    * Span category for persistence operations causally related by client interactions.
    */
   PERSISTENCE,

   /**
    * Span category for security operations causally related by client interactions.
    */
   SECURITY;

   private final String name;

   SpanCategory() {
      this.name = NamingStrategy.KEBAB_CASE.convert(name().toLowerCase());
   }

   @Override
   public String toString() {
      return name;
   }

   public static SpanCategory fromString(String value) {
      return stream(SpanCategory.values())
              .filter(p -> p.name.equalsIgnoreCase(value))
              .findFirst().orElse(null);
   }
}
