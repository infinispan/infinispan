package org.infinispan.persistence.jpa.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the JPA cache stores configuration
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   PERSISTENCE_UNIT_NAME("persistence-unit"),
   ENTITY_CLASS_NAME("entity-class"),
   BATCH_SIZE("batch-size"),
   STORE_METADATA("store-metadata")
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
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
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
