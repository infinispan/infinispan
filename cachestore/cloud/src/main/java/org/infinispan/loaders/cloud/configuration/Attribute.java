package org.infinispan.loaders.cloud.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the Cloud cache store configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   BUCKET_PREFIX("bucketPrefix"),
   CLOUD_SERVICE("cloudService"),
   CLOUD_SERVICE_LOCATION("cloudServiceLocation"),
   COMPRESS("compress"),
   IDENTITY("identity"),
   LAZY_PURGING_ONLY("lazyPurgingOnly"),
   MAX_CONNECTIONS("maxConnections"),
   PASSWORD("password"),
   PROXY_HOST("proxyHost"),
   PROXY_PORT("proxyPort"),
   REQUEST_TIMEOUT("requestTimeout"),
   SECURE("secure"),

   ;

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
}
