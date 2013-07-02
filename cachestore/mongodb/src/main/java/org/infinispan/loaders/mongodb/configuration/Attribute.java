package org.infinispan.loaders.mongodb.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * All valid attributes used to configure a MongoDB cachestore
 * Refer to {@link Element} to have the list of available configuration elements
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
public enum Attribute {
   UNKNOWN(null),

   /**
    * Attributes of Element.CONNECTION
    */
   HOST("host"),
   PORT("port"),
   TIMEOUT("timeout"),
   ACKNOWLEDGMENT("acknowledgment"),

   /**
    * Attributes of Element.AUTHENTICATION
    */
   USERNAME("username"),
   PASSWORD("password"),

   /**
    * Attributes of Element.STORAGE
    */
   DATABASE("database"),
   COLLECTION("collection");

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   /**
    * @return the name of the attribute
    */
   public String getName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.getName();
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
