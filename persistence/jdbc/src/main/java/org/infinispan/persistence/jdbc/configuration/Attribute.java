package org.infinispan.persistence.jdbc.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the JDBC cache stores configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   BATCH_SIZE("batchSize"),
   CONNECTION_URL("connectionUrl"),
   CREATE_ON_START("createOnStart"),
   JNDI_URL("jndiUrl"),
   DRIVER_CLASS("driverClass"),
   DROP_ON_EXIT("dropOnExit"),
   FETCH_SIZE("fetchSize"),
   KEY_TO_STRING_MAPPER("key2StringMapper"),
   NAME("name"),
   PASSIVATION("passivation"),
   PASSWORD("password"),
   PREFIX("prefix"),
   PRELOAD("true"),
   TYPE("type"),
   USERNAME("username")
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
