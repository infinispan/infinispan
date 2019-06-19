package org.infinispan.server.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Attribute {
   UNKNOWN(null), // must be first

   ALIAS,
   CACHE_CONTAINER,
   DEFAULT_INTERFACE,
   DIGEST_REALM_NAME,
   DIRECT_VERIFICATION,
   ENABLED_CIPHERSUITES,
   ENABLED_PROTOCOLS,
   GENERATE_SELF_SIGNED_CERTIFICATE_HOST,
   GROUPS_ATTRIBUTE,
   IDLE_TIMEOUT,
   INTERFACE,
   IO_THREADS,
   KEYSTORE_PASSWORD,
   KEYTAB,
   KEY_PASSWORD,
   NAME, PATH, PORT,
   PLAIN_TEXT,
   PORT_OFFSET,
   PROVIDER,
   RECEIVE_BUFFER_SIZE,
   RELATIVE_TO,
   REQUIRE_SSL_CLIENT_AUTH,
   SECURITY_REALM,
   SEND_BUFFER_SIZE,
   SOCKET_BINDING,
   TCP_KEEPALIVE,
   TCP_NODELAY,
   VALUE,
   WORKER_THREADS,
   URL, PRINCIPAL, CREDENTIAL, PAGE_SIZE, SEARCH_DN, RDN_IDENTIFIER, FROM, TO, FILTER,
   FILTER_BASE_DN, WRITABLE, VERIFIABLE, PRINCIPAL_CLAIM, CLIENT_ID, CLIENT_SECRET, INTROSPECTION_URL, CLIENT_SSL_CONTEXT, HOST_NAME_VERIFICATION_POLICY, ISSUER, AUDIENCE, PUBLIC_KEY, LEVELS, ENCODED;

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
