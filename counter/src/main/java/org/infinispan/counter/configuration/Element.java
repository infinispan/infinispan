package org.infinispan.counter.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
public enum Element {
   //must be first
   UNKNOWN(null),

   COUNTERS("counters"),
   LOWER_BOUND("lower-bound"),
   STRONG_COUNTER("strong-counter"),
   UPPER_BOUND("upper-bound"),
   WEAK_COUNTER("weak-counter"),
   ;

   private static final Map<String, Element> ELEMENTS;

   static {
      final Map<String, Element> map = new HashMap<>(8);
      for (Element element : values()) {
         final String name = element.name;
         if (name != null) {
            map.put(name, element);
         }
      }
      ELEMENTS = map;
   }

   private final String name;

   Element(final String name) {
      this.name = name;
   }

   public static Element forName(final String localName) {
      final Element element = ELEMENTS.get(localName);
      return element == null ? UNKNOWN : element;
   }

   @Override
   public String toString() {
      return name;
   }
}
