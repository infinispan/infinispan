package org.infinispan.persistence.dummy;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the Dummy cache store configuration
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   STORE_NAME("store-name"),
   START_FAILURES("start-failures"),

   SLOW("slow");

   private final String name;

   Attribute(final String name) {
      this.name = name;
   }

   /**
    * Get the local name of this element.
    *
    * @return the local name
    */
   public String getLocalName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      Map<String, Attribute> map = new HashMap<>();
      for (Attribute attribute : values()) {
         final String name = attribute.getLocalName();
         if (name != null) {
            map.put(name, attribute);
         }
      }
      attributes = map;
   }

   public static Attribute forName(final String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }

   @Override
   public String toString() {
      return name;
   }
}
