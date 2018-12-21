package org.infinispan.server.server.configuration.hotrod;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Element {
   //must be first
   UNKNOWN(null),

   HOTROD_CONNECTOR,
   AUTHENTICATION,
   ENCRYPTION,
   SASL,
   SNI,
   TOPOLOGY_STATE_TRANSFER,
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

   Element() {
      this.name = name().toLowerCase().replace('_', '-');
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
