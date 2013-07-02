package org.infinispan.loaders.bdbje.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the Bdbje cache store configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   CACHE_DB_NAME_PREFIX("cacheDbNamePrefix"),
   CATALOG_DB_NAME("catalogDbName"),
   ENVIRONMENT_PROPERTIES_FILE("environmentPropertiesFile"),
   EXPIRY_DB_PREFIX("expiryDbPrefix"),
   LOCATION("location"),
   LOCK_ACQUISITION_TIMEOUT("lockAcquisitionTimeout"),
   MAX_TX_RETRIES("maxTxRetries"),
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
