package org.infinispan.persistence.rocksdb.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the LevelDB cache stores configuration
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   BLOCK_SIZE("block-size"),
   CACHE_SIZE("cache-size"),
   CLEAR_THRESHOLD("clear-threshold"),
   COMPRESSION_TYPE("compressionType"),
   EXPIRED_LOCATION("expiredLocation"),
   EXPIRY_QUEUE_SIZE("expiryQueueSize"),
   IMPLEMENTATION_TYPE("implementationType"),
   LOCATION("location"),
   PATH("path"),
   RELATIVE_TO("relative-to"),
   QUEUE_SIZE("queue-size"),
   TYPE("type"),
   ;

   private final String name;

   Attribute(final String name) {
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
