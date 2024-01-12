package ${package};

import java.util.HashMap;
import java.util.Map;

public enum Attribute {
   // must be first
   UNKNOWN(null),

   MESSAGE("message");
   private static final Map<String, Attribute> ATTRIBUTES;

   static {
      final Map<String, Attribute> map = new HashMap<>();
      for (Attribute attribute : values()) {
         final String name = attribute.name;
         if (name != null) {
            map.put(name, attribute);
         }
      }
      ATTRIBUTES = Map.copyOf(map);
   }

   private final String name;

   Attribute(final String name) {
      this.name = name;
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
