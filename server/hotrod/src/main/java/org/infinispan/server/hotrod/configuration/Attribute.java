package org.infinispan.server.hotrod.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Attribute {
   UNKNOWN(null), // must be first

   AWAIT_INITIAL_RETRIEVAL,
   EXTERNAL_HOST,
   EXTERNAL_PORT,
   HOST_NAME,
   @Deprecated(forRemoval = true, since = "11.0")
   LAZY_RETRIEVAL,
   LOCK_TIMEOUT,
   MECHANISMS,
   NAME,
   NETWORK_PREFIX_OVERRIDE,
   POLICY,
   QOP,
   REPLICATION_TIMEOUT,
   REQUIRE_SSL_CLIENT_AUTH,
   SECURITY_REALM,
   SERVER_PRINCIPAL,
   SERVER_NAME,
   SOCKET_BINDING,
   TOPOLOGY_STATE_TRANSFER,
   STRENGTH,
   VALUE;

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
