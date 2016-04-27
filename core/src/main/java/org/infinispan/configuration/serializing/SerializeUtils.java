package org.infinispan.configuration.serializing;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;

public class SerializeUtils {
   private SerializeUtils() {}

   public static void writeOptional(XMLExtendedStreamWriter writer, Enum<?> attribute, String value) throws XMLStreamException {
      if (value != null) {
         writer.writeAttribute(attribute, value);
      }
   }

   public static void writeTypedProperties(XMLExtendedStreamWriter writer, TypedProperties properties) throws XMLStreamException {
      if (!properties.isEmpty()) {
         for (String property : properties.stringPropertyNames()) {
            writer.writeStartElement(Element.PROPERTY);
            writer.writeAttribute(Attribute.NAME, property);
            writer.writeCharacters(properties.getProperty(property));
            writer.writeEndElement();
         }
      }
   }
}
