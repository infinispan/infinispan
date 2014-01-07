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

   APPEND_CACHE_NAME_TO_PATH("append-cache-name-to-path"),
   BUFFER_SIZE("buffer-size"),
   CONNECTION_TIMEOUT("connection-timeout"),
   HOST("host"),
   KEY_TO_STRING_MAPPER("key-to-string-mapper"),
   MAX_CONNECTIONS_PER_HOST("max-connections-per-host"),
   MAX_TOTAL_CONNECTIONS("max-total-connections"),
   OUTBOUND_SOCKET_BINDING("outbound-socket-binding"),
   PATH("path"),
   PORT("port"),
   SOCKET_TIMEOUT("socket-timeout"),
   TCP_NO_DELAY("tcp-no-delay"),
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
