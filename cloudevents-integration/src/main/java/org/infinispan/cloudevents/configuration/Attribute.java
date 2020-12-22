package org.infinispan.cloudevents.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Attribute.
 *
 * @author Dan Berindei
 * @since 12
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   BOOTSTRAP_SERVERS("bootstrap-servers"),
   ACKS("acks"),
   AUDIT_TOPIC("audit-topic"),
   CACHE_ENTRIES_TOPIC("cache-entries-topic"),
   ENABLED("enabled"),
   ;

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   /**
    * Get the local name of this element.
    *
    * @return the local name
    */
   public String getLocalName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      Map<String, Attribute> map = new HashMap<>();
      for (Attribute attribute : values()) {
         final String name = attribute.getLocalName();
         if (name != null) {
            map.put(name, attribute);
         }
      }
      attributes = map;
   }

   public static Attribute forName(final String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }
}
