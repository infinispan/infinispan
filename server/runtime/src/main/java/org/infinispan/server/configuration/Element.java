package org.infinispan.server.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Element {
   UNKNOWN(null), //must be first

   GLOBAL,
   INET_ADDRESS,
   INTERFACE,
   INTERFACES,
   KEYSTORE,
   LINK_LOCAL,
   LOOPBACK,
   MATCH_ADDRESS,
   MATCH_HOST,
   MATCH_INTERFACE,
   NON_LOOPBACK,
   PATH,
   PATHS,
   PROPERTIES,
   SECURITY,
   SECURITY_REALM,
   SECURITY_REALMS,
   SERVER,
   SERVER_IDENTITIES,
   SITE_LOCAL,
   SOCKET_BINDING,
   SOCKET_BINDINGS,
   SSL,
   ENDPOINTS,
   TRUSTSTORE,
   LOCAL,
   KERBEROS,
   ENGINE,
   PROPERTIES_REALM, KERBEROS_REALM(), LOCAL_REALM(), USER_PROPERTIES(), GROUP_PROPERTIES(), TRUSTSTORE_REALM();

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
