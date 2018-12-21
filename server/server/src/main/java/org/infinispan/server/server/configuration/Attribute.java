package org.infinispan.server.server.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   DEFAULT_INTERFACE,
   INTERFACE,
   NAME,
   PATH,
   PORT,
   PORT_OFFSET,
   VALUE,
   RELATIVE_TO,
   KEYSTORE_PASSWORD, ALIAS,
   KEY_PASSWORD,
   GENERATE_SELF_SIGNED_CERTIFICATE_HOST,
   ENABLED_PROTOCOLS,
   ENABLED_CIPHERSUITES, KEYTAB();

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
