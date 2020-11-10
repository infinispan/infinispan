package org.infinispan.server.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Element {
   UNKNOWN(null), //must be first

   ANY_ADDRESS,
   ATTRIBUTE,
   ATTRIBUTE_MAPPING,
   ATTRIBUTE_REFERENCE,
   ENDPOINTS,
   ENGINE,
   FILESYSTEM_REALM,
   GLOBAL,
   GROUP_PROPERTIES,
   IDENTITY_MAPPING,
   INET_ADDRESS,
   INTERFACE,
   INTERFACES,
   JWT,
   KERBEROS,
   KEYSTORE,
   LDAP_REALM,
   LINK_LOCAL,
   LOCAL,
   LOCAL_REALM,
   LOOPBACK,
   MATCH_ADDRESS,
   MATCH_HOST,
   MATCH_INTERFACE,
   NAME_REWRITER,
   NON_LOOPBACK,
   OAUTH2_INTROSPECTION,
   OPTION,
   PATH,
   PATHS,
   PROPERTIES,
   PROPERTIES_REALM,
   REGEX_PRINCIPAL_TRANSFORMER,
   SECURITY,
   SECURITY_REALM,
   SECURITY_REALMS,
   SERVER,
   SERVER_IDENTITIES,
   SITE_LOCAL,
   SOCKET_BINDING,
   SOCKET_BINDINGS,
   SSL,
   TOKEN_REALM,
   TRUSTSTORE,
   TRUSTSTORE_REALM,
   USER_PASSWORD_MAPPER,
   USER_PROPERTIES,
   DATA_SOURCES,
   DATA_SOURCE,
   CONNECTION_FACTORY,
   CONNECTION_POOL,
   CONNECTION_PROPERTY,
   CREDENTIAL_STORES,
   CREDENTIAL_STORE,
   CREDENTIAL_REFERENCE,
   CLEAR_TEXT_CREDENTIAL;

   private static final Map<String, Element> ELEMENTS;

   static {
      final Map<String, Element> map = new HashMap<>(8);
      for (Element element : values()) {
         final String name = element.name;
         if (name != null) {
            map.put(name, element);
         }
      }
      ELEMENTS = map;
   }

   private final String name;

   Element(String name) {
      this.name = name;
   }

   Element() {
      this.name = name().toLowerCase().replace('_', '-');
   }

   public static Element forName(final String localName) {
      final Element element = ELEMENTS.get(localName);
      return element == null ? UNKNOWN : element;
   }

   @Override
   public String toString() {
      return name;
   }
}
