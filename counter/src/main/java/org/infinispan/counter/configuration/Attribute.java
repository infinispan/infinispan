package org.infinispan.counter.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   CONCURRENCY_LEVEL("concurrency-level"),
   INITIAL_VALUE("initial-value"),
   NAME("name"),
   NUM_OWNERS("num-owners"),
   RELIABILITY("reliability"),
   STORAGE("storage"),
   VALUE("value"),;
   private static final Map<String, Attribute> ATTRIBUTES;

   static {
      final Map<String, Attribute> map = new HashMap<>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.name;
         if (name != null) {
            map.put(name, attribute);
         }
      }
      ATTRIBUTES = map;
   }

   private final String name;

   Attribute(final String name) {
      this.name = name;
   }

   public static Attribute forName(String localName) {
      final Attribute attribute = ATTRIBUTES.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }

   @Override
   public String toString() {
      return name;
   }
}
