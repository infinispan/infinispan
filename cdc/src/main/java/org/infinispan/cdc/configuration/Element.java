package org.infinispan.cdc.configuration;

import java.util.HashMap;
import java.util.Map;

enum Element {
   UNKNOWN(null),
   CDC("change-data-capture"),
   TABLE("table"),
   PRIMARY_KEY("primary-key"),
   COLUMN("column"),
   CONNECTOR_PROPERTIES("connector-properties"),
   ;

   private final String name;

   Element(String name) {
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

   private static final Map<String, Element> MAP;

   static {
      final Map<String, Element> map = new HashMap<String, Element>(8);
      for (Element element : values()) {
         final String name = element.getLocalName();
         if (name != null) {
            map.put(name, element);
         }
      }
      MAP = map;
   }

   public static Element forName(final String localName) {
      final Element element = MAP.get(localName);
      return element == null ? UNKNOWN : element;
   }

   @Override
   public String toString() {
      return name;
   }
}
