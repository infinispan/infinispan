package org.infinispan.server.core.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Attribute {
   UNKNOWN(null), // must be first

   CACHE,
   HOST,
   IDLE_TIMEOUT,
   IGNORED_CACHES,
   IO_THREADS,
   MECHANISMS,
   NAME,
   POLICY,
   PORT,
   QOP,
   RECEIVE_BUFFER_SIZE,
   REQUIRE_SSL_CLIENT_AUTH,
   SECURITY_REALM,
   SEND_BUFFER_SIZE,
   SERVER_NAME,
   SERVER_PRINCIPAL,
   SOCKET_BINDING,
   START_TRANSPORT,
   STRENGTH,
   TCP_KEEPALIVE,
   TCP_NODELAY,
   VALUE,
   ZERO_CAPACITY_NODE
   ;

   private static final Map<String, Attribute> ATTRIBUTES;

   static {
      final Map<String, Attribute> map = new HashMap<>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.name;
         if (name != null) {
            map.put(name, attribute);
         }
      }
      ATTRIBUTES = map;
   }

   private final String name;

   Attribute(final String name) {
      this.name = name;
   }

   Attribute() {
      this.name = name().toLowerCase().replace('_', '-');
   }

   public static Attribute forName(String localName) {
      final Attribute attribute = ATTRIBUTES.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }

   @Override
   public String toString() {
      return name;
   }
}
