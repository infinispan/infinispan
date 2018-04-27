package org.infinispan.tools.store.migrator;

import java.util.Properties;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
enum Element {

   BATCH("batch"),
   BINARY("binary"),
   CACHE_NAME("cache_name"),
   CLASS("class"),
   CONNECTION_URL("connection_url"),
   CONNECTION_POOL("connection_pool"),
   DATA("data"),
   DB("db"),
   DIALECT("dialect"),
   DISABLE_INDEXING("disable_indexing"),
   DISABLE_UPSERT("disable_upsert"),
   DRIVER_CLASS("driver_class"),
   EXTERNALIZERS("externalizers"),
   ID("id"),
   KEY_TO_STRING_MAPPER("key_to_string_mapper"),
   MAJOR_VERSION("major_version"),
   MINOR_VERSION("minor_version"),
   MARSHALLER("marshaller"),
   NAME("name"),
   PASSWORD("password"),
   SOURCE("source"),
   SIZE("size"),
   STRING("string"),
   TARGET("target"),
   TABLE("table"),
   TABLE_NAME_PREFIX("table_name_prefix"),
   TIMESTAMP("timestamp"),
   TYPE("type"),
   USERNAME("username");

   private final String name;

   Element(final String name) {
      this.name = name;
   }

   @Override
   public String toString() {
      return name;
   }

   static String property(Properties properties, Element... elements) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < elements.length; i++) {
         sb.append(elements[i].toString());
         if (i != elements.length - 1) sb.append(".");
      }
      return properties.getProperty(sb.toString());
   }
}
