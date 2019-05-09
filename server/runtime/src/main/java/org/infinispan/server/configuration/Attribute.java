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
   DEFAULT_INTERFACE,
   DIGEST_REALM_NAME,
   ENABLED_CIPHERSUITES,
   ENABLED_PROTOCOLS,
   GENERATE_SELF_SIGNED_CERTIFICATE_HOST,
   GROUPS_ATTRIBUTE,
   INTERFACE,
   KEYSTORE_PASSWORD,
   KEYTAB,
   KEY_PASSWORD,
   NAME, PATH, PORT,
   PLAIN_TEXT,
   PORT_OFFSET,
   PROVIDER,
   RELATIVE_TO,
   VALUE,
   DIR_CONTEXT, DIRECT_VERIFICATION, ALLOW_BLANK_PASSWORD;

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
