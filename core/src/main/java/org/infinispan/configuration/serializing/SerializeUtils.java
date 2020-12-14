package org.infinispan.configuration.serializing;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

public class SerializeUtils {
   private SerializeUtils() {}

   public static void writeOptional(ConfigurationWriter writer, Enum<?> attribute, String value) {
      if (value != null) {
         writer.writeAttribute(attribute, value);
      }
   }

   public static void writeTypedProperties(ConfigurationWriter writer, TypedProperties properties) {
      if (!properties.isEmpty()) {
         writer.writeStartListElement(Element.PROPERTIES, false);
         for (String property : properties.stringPropertyNames()) {
            writer.writeStartElement(Element.PROPERTY);
            writer.writeAttribute(Attribute.NAME, property);
            writer.writeCharacters(properties.getProperty(property));
            writer.writeEndElement();
         }
         writer.writeEndListElement();
      }
   }
}
