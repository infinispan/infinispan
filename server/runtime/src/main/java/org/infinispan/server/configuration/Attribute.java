package org.infinispan.server.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Attribute {
   UNKNOWN(null), // must be first

   ADMIN,
   ALIAS,
   AUDIENCE,
   AUTH_SERVER_URL,
   BACKGROUND_VALIDATION,
   BLOCKING_TIMEOUT,
   CACHE_CONTAINER,
   CACHE_LIFESPAN,
   CACHE_MAX_SIZE,
   CLEAR_TEXT,
   CLIENT_ID,
   CLIENT_SECRET,
   CLIENT_SSL_CONTEXT,
   COMMAND,
   CONNECTION_POOLING,
   CONNECTION_TIMEOUT,
   CREDENTIAL,
   DEBUG,
   DEFAULT_INTERFACE,
   DEFAULT_REALM,
   DIGEST_REALM_NAME,
   DIRECT_VERIFICATION,
   DRIVER,
   ENABLED_CIPHERSUITES,
   ENABLED_CIPHERSUITES_TLS13,
   ENABLED_PROTOCOLS,
   EXTRACT_RDN,
   FAIL_CACHE,
   FILTER,
   FILTER_DN,
   FILTER_NAME,
   FROM,
   GENERATE_SELF_SIGNED_CERTIFICATE_HOST,
   GROUPS_ATTRIBUTE,
   HOST_NAME_VERIFICATION_POLICY,
   IDLE_REMOVAL,
   IDLE_TIMEOUT,
   INITIAL_SIZE,
   INTERFACE,
   INTROSPECTION_URL,
   IO_THREADS,
   ISSUER,
   JKU_TIMEOUT,
   JNDI_NAME,
   @Deprecated
   KEYSTORE_PASSWORD,
   KEYTAB_PATH,
   KEY_PASSWORD,
   LEAK_DETECTION,
   LEVELS,
   MASKED,
   MAX_SIZE,
   MECHANISM_NAMES,
   MECHANISM_OIDS,
   METRICS_AUTH,
   MINIMUM_REMAINING_LIFETIME,
   MIN_SIZE,
   NAME,
   NEW_CONNECTION_SQL,
   OBTAIN_KERBEROS_TICKET,
   PAGE_SIZE,
   PASSWORD,
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
   REPLACEMENT,
   REPLACE_ALL,
   REQUEST_LIFETIME,
   REQUIRED,
   REQUIRE_SSL_CLIENT_AUTH,
   ROLE_RECURSION,
   ROLE_RECURSION_NAME,
   SEARCH_DN,
   SEARCH_RECURSIVE,
   SEARCH_TIME_LIMIT,
   SECURITY_REALM,
   SEND_BUFFER_SIZE,
   SERVER,
   SOCKET_BINDING,
   STATISTICS,
   STORE,
   TCP_KEEPALIVE,
   TCP_NODELAY,
   TO,
   TRANSACTION_ISOLATION,
   TYPE,
   URL,
   USERNAME,
   VALIDATE_ON_ACQUISITION,
   VALUE,
   VERIFIABLE,
   WORKER_THREADS,
   WRAP_GSS_CREDENTIAL,
   WRITABLE;

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
