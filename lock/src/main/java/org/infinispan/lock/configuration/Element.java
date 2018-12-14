package org.infinispan.lock.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
public enum Element {
   //must be first
   UNKNOWN(null),

   CLUSTERED_LOCKS("clustered-locks"),
   CLUSTERED_LOCK("clustered-lock"),
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
