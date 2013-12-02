package org.infinispan.persistence.rest.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the Remote cache store configuration
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   APPEND_CACHE_NAME_TO_PATH("appendCacheNameToPath"),
   BUFFER_SIZE("bufferSize"),
   CONNECTION_TIMEOUT("connectionTimeout"),
   HOST("host"),
   KEY_TO_STRING_MAPPER("key2StringMapper"),
   MAX_CONNECTIONS_PER_HOST("maxConnectionsPerHost"),
   MAX_TOTAL_CONNECTIONS("maxTotalConnections"),
   PATH("path"),
   PORT("port"),
   SOCKET_TIMEOUT("socketTimeout"),
   TCP_NO_DELAY("tcpNoDelay"),
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
