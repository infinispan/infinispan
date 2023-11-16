package org.infinispan.persistence.jdbc.common.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * An enumeration of all the recognized XML element local names for the JDBC cache stores
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Element {
   // must be first
   UNKNOWN(null),

   STRING_KEYED_JDBC_STORE,
   BINARY_KEYED_JDBC_STORE,
   MIXED_KEYED_JDBC_STORE,
   TABLE_JDBC_STORE,
   QUERY_JDBC_STORE,

   CONNECTION_POOL,
   CDI_DATA_SOURCE,
   DATA_SOURCE,
   SIMPLE_CONNECTION,

   STRING_KEYED_TABLE,

   DATA_COLUMN,
   ID_COLUMN,
   TIMESTAMP_COLUMN,
   SEGMENT_COLUMN,

   SCHEMA,

   QUERIES,
   SELECT_SINGLE,
   SELECT_ALL,
   DELETE_SINGLE,
   DELETE_ALL,
   UPSERT,
   SIZE,
   ;

   private final String name;

   Element(final String name) {
      this.name = name;
   }

   Element() {
      this.name = name().toLowerCase().replace('_', '-');
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
      final Map<String, Element> map = new HashMap<>(8);
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
