package org.infinispan.server.configuration.rest;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   ALLOW_CREDENTIALS,
   COMPRESSION_LEVEL,
   CONTEXT_PATH,
   EXTENDED_HEADERS,
   HOST_NAME,
   MAX_AGE_SECONDS,
   MAX_CONTENT_LENGTH,
   MECHANISMS,
   NAME,
   REQUIRE_SSL_CLIENT_AUTH,
   SECURITY_REALM,
   SERVER_PRINCIPAL,
   SOCKET_BINDING,
   ALLOWED_HEADERS,
   ALLOWED_ORIGINS,
   ALLOWED_METHODS,
   EXPOSE_HEADERS;

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
