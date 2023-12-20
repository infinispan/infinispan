package ${package};

import java.util.HashMap;
import java.util.Map;

public enum Element {
   //must be first
   UNKNOWN(null),

   ROOT("custom-module"),
   ;

   private static final Map<String, Element> ELEMENTS;

   static {
      final Map<String, Element> map = new HashMap<>();
      for (Element element : values()) {
         final String name = element.name;
         if (name != null) {
            map.put(name, element);
         }
      }
      ELEMENTS = Map.copyOf(map);
   }

   private final String name;

   Element(final String name) {
      this.name = name;
   }

   public static Element forName(final String localName) {
      final Element element = ELEMENTS.get(localName);
      return element == null ? UNKNOWN : element;
   }

   @Override
   public String toString() {
      return name;
   }
}
