package org.infinispan.loaders.cassandra.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the Cassandra cache store configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),
   AUTO_CREATE_KEYSPACE("autoCreateKeyspace"),
   CONFIGURATION_PROPERTIES_FILE("configurationPropertiesFile"),
   ENTRY_COLUMN_FAMILY("entryColumnFamily"),
   EXPIRATION_COLUMN_FAMILY("expirationColumnFamily"),
   FRAMED("framed"),
   HOST("host"),
   KEY_MAPPER("keyMapper"),
   KEY_SPACE("keySpace"),
   PASSWORD("PASSWORD"),
   PORT("port"),
   USERNAME("USERNAME"),
   READ_CONSISTENCY_LEVEL("readConsistencyLevel"),
   WRITE_CONSISTENCY_LEVEL("writeConsistencyLevel")
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
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
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
}
