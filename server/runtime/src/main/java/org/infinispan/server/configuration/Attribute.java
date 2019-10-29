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
   AUDIENCE,
   CACHE_CONTAINER,
   CLIENT_ID,
   CLIENT_SECRET,
   CLIENT_SSL_CONTEXT,
   CREDENTIAL,
   DEBUG,
   DEFAULT_INTERFACE,
   DIGEST_REALM_NAME,
   DIRECT_VERIFICATION,
   ENABLED_CIPHERSUITES,
   ENABLED_PROTOCOLS,
   ENCODED,
   FAIL_CACHE,
   FILTER,
   FILTER_DN,
   FROM,
   GENERATE_SELF_SIGNED_CERTIFICATE_HOST,
   GROUPS_ATTRIBUTE,
   HOST_NAME_VERIFICATION_POLICY,
   IDLE_TIMEOUT,
   INTERFACE,
   INTROSPECTION_URL,
   IO_THREADS,
   ISSUER,
   JKU_TIMEOUT,
   KEYSTORE_PASSWORD,
   KEYTAB_PATH,
   KEY_PASSWORD,
   LEVELS,
   MECHANISM_NAMES,
   MECHANISM_OIDS,
   MINIMUM_REMAINING_LIFETIME,
   NAME,
   OBTAIN_KERBEROS_TICKET,
   PAGE_SIZE,
   PATH,
   PATTERN,
   PLAIN_TEXT,
   PORT,
   PORT_OFFSET,
   PRINCIPAL,
   PRINCIPAL_CLAIM,
   PROVIDER,
   PUBLIC_KEY,
   RDN_IDENTIFIER,
   RECEIVE_BUFFER_SIZE,
   RELATIVE_TO,
   REPLACE_ALL,
   REQUEST_LIFETIME,
   REQUIRE_SSL_CLIENT_AUTH,
   REQUIRED,
   REPLACEMENT,
   SEARCH_DN,
   SECURITY_REALM,
   SEND_BUFFER_SIZE,
   SERVER,
   SOCKET_BINDING,
   TCP_KEEPALIVE,
   TCP_NODELAY,
   TO,
   URL,
   VALUE,
   VERIFIABLE,
   WORKER_THREADS,
   WRAP_GSS_CREDENTIAL,
   WRITABLE,
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
