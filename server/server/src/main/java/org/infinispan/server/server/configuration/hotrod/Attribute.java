package org.infinispan.server.server.configuration.hotrod;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Attribute {
   UNKNOWN(null), // must be first

   AWAIT_INITIAL_RETRIEVAL,
   CACHE_CONTAINER,
   EXTERNAL_HOST,
   EXTERNAL_PORT,
   IDLE_TIMEOUT,
   IO_THREADS,
   LAZY_RETRIEVAL,
   LOCK_TIMEOUT,
   MECHANISMS,
   NAME,
   QOP,
   RECEIVE_BUFFER_SIZE,
   REPLICATION_TIMEOUT,
   REQUIRE_SSL_CLIENT_AUTH,
   SECURITY_REALM,
   SEND_BUFFER_SIZE,
   SERVER_CONTEXT_NAME,
   SERVER_NAME,
   SOCKET_BINDING,
   STRENGTH,
   TCP_KEEPALIVE,
   TCP_NODELAY,
   WORKER_THREADS,
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
