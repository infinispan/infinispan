package org.infinispan.persistence.jdbc.common.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the JDBC cache stores configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),
   ANNOTATION,
   BATCH_SIZE,
   CONNECTION_URL,
   CREATE_ON_START,
   DIALECT,
   DB_MAJOR_VERSION,
   DB_MINOR_VERSION,
   DELETE_ALL,
   DELETE_SINGLE,
   JNDI_URL,
   DRIVER,
   DROP_ON_EXIT,
   EMBEDDED_KEY,
   FETCH_SIZE,
   FILE_NAME,
   KEY_COLUMNS,
   KEY_MESSAGE_NAME,
   KEY_TO_STRING_MAPPER,
   MESSAGE_NAME,
   NAME,
   PACKAGE,
   PASSIVATION,
   PASSWORD,
   PREFIX,
   PROPERTIES_FILE,
   READ_QUERY_TIMEOUT,
   SELECT_ALL,
   SELECT_SINGLE,
   SIZE,
   TABLE_NAME,
   TYPE,
   UPSERT,
   USERNAME,
   WRITE_QUERY_TIMEOUT,
   ;

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   Attribute() {
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
