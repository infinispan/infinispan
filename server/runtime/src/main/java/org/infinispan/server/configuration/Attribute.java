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
   AUTH_SERVER_URL,
   CACHE_CONTAINER,
   CACHE_LIFESPAN,
   CACHE_MAX_SIZE,
   CLIENT_ID,
   CLIENT_SECRET,
   CLIENT_SSL_CONTEXT,
   CONNECTION_POOLING,
   CONNECTION_TIMEOUT,
   CREDENTIAL,
   DEBUG,
   DEFAULT_INTERFACE,
   DEFAULT_REALM,
   DIGEST_REALM_NAME,
   DIRECT_VERIFICATION,
   ENABLED_CIPHERSUITES,
   ENABLED_PROTOCOLS,
   ENCODED,
   EXTRACT_RDN,
   FAIL_CACHE,
   FILTER,
   FILTER_DN,
   FILTER_NAME,
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
   JNDI_NAME,
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
   READ_TIMEOUT,
   REALMS,
   RECEIVE_BUFFER_SIZE,
   REFERENCE,
   REFERRAL_MODE,
   RELATIVE_TO,
   REPLACE_ALL,
   REQUEST_LIFETIME,
   REQUIRE_SSL_CLIENT_AUTH,
   REQUIRED,
   REPLACEMENT,
   ROLE_RECURSION,
   ROLE_RECURSION_NAME,
   SEARCH_DN,
   SEARCH_RECURSIVE,
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
   STATISTICS,
   DRIVER,
   USERNAME,
   PASSWORD,
   TRANSACTION_ISOLATION,
   NEW_CONNECTION_SQL,
   MAX_SIZE,
   MIN_SIZE,
   INITIAL_SIZE,
   BLOCKING_TIMEOUT,
   BACKGROUND_VALIDATION,
   VALIDATE_ON_ACQUISITION,
   LEAK_DETECTION,
   IDLE_REMOVAL,
   ADMIN,
   METRICS_AUTH,
   CLEAR_TEXT,
   STORE,
   TYPE,
   SEARCH_TIME_LIMIT;

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
