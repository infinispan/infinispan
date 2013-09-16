package org.infinispan.compatibility.parsing;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the JDBC cache stores configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum AttributeJdbc {
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

   private AttributeJdbc(final String name) {
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

   private static final Map<String, AttributeJdbc> attributes;

   static {
      final Map<String, AttributeJdbc> map = new HashMap<String, AttributeJdbc>(64);
      for (AttributeJdbc attributeJdbc : values()) {
         final String name = attributeJdbc.getLocalName();
         if (name != null) {
            map.put(name, attributeJdbc);
         }
      }
      attributes = map;
   }

   public static AttributeJdbc forName(final String localName) {
      final AttributeJdbc attributeJdbc = attributes.get(localName);
      return attributeJdbc == null ? UNKNOWN : attributeJdbc;
   }
}
