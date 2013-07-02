package org.infinispan.loaders.mongodb.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * All valid elements to configure a MongoDB cachestore
 * See also {@link Attribute} to have the complete list of attributes
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
public enum Element {
   UNKNOWN(null),
   MONGODB_STORE("mongodbStore"),
   CONNECTION("connection"),
   AUTHENTICATION("authentication"),
   STORAGE("storage");

   private final String name;


   Element(final String name) {
      this.name = name;
   }

   /**
    * Get the name of the current element
    *
    * @return the name
    */
   public String getName() {
      return name;
   }

   private static final Map<String, Element> elements;

   static {
      final Map<String, Element> map = new HashMap<String, Element>(8);
      for (Element element : values()) {
         final String name = element.getName();
         if (name != null) {
            map.put(name, element);
         }
      }
      elements = map;
   }

   public static Element forName(final String localName) {
      final Element element = elements.get(localName);
      return element == null ? UNKNOWN : element;
   }
}
