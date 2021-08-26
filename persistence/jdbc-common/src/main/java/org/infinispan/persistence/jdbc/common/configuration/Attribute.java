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

   BATCH_SIZE("batch-size"),
   CONNECTION_URL("connection-url"),
   CREATE_ON_START("create-on-start"),
   DIALECT("dialect"),
   DB_MAJOR_VERSION("db-major-version"),
   DB_MINOR_VERSION("db-minor-version"),
   DELETE_ALL("delete-all"),
   DELETE_SINGLE("delete-single"),
   JNDI_URL("jndi-url"),
   DRIVER_CLASS("driver"),
   DROP_ON_EXIT("drop-on-exit"),
   EMBEDDED_KEY("embedded-key"),
   FETCH_SIZE("fetch-size"),
   FILE_NAME("file-name"),
   KEY_COLUMNS("key-columns"),
   KEY_MESSAGE_NAME("key-message-name"),
   KEY_TO_STRING_MAPPER("key-to-string-mapper"),
   MESSAGE_NAME("message-name"),
   NAME("name"),
   PACKAGE("package"),
   PASSIVATION("passivation"),
   PASSWORD("password"),
   PREFIX("prefix"),
   PROPERTIES_FILE("properties-file"),
   READ_QUERY_TIMEOUT("read-query-timeout"),
   SELECT_ALL("select-all"),
   SELECT_SINGLE("select-single"),
   SIZE("size"),
   TABLE_NAME("table-name"),
   TYPE("type"),
   UPSERT("upsert"),
   USERNAME("username"),
   WRITE_QUERY_TIMEOUT("write-query-timeout"),
   ;

   private final String name;

   private Attribute(final String name) {
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
