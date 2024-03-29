package org.infinispan.server.configuration.resp;

import java.util.HashMap;
import java.util.Map;

/**
 * @author William Burns
 * @since 14.0
 */
public enum Attribute {
   UNKNOWN(null), // must be first

   CACHE,
   NAME,
   SECURITY_REALM,
   SOCKET_BINDING;

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
