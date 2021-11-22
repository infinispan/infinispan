package org.infinispan.configuration.serializing;

import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
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
      writeTypedProperties(writer, properties, Element.PROPERTIES, Element.PROPERTY);
   }

   public static void writeTypedProperties(ConfigurationWriter writer, TypedProperties properties, Enum<?> collectionElement, Enum<?> itemElement) {
      if (!properties.isEmpty()) {
         if (writer.hasFeature(ConfigurationFormatFeature.BARE_COLLECTIONS)) {
            for(String property : properties.stringPropertyNames()) {
               writer.writeStartElement(itemElement);
               writer.writeAttribute(Attribute.NAME, property);
               writer.writeCharacters(properties.getProperty(property));
               writer.writeEndElement();
            }
         } else {
            writer.writeStartMap(collectionElement);
            for (String property : properties.stringPropertyNames()) {
               writer.writeMapItem(itemElement, Attribute.NAME, property, properties.getProperty(property));
            }
            writer.writeEndMap();
         }
      }
   }
}
