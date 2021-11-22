package org.infinispan.commons.configuration.attributes;

import java.util.Map;

import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
import org.infinispan.commons.configuration.io.ConfigurationWriter;

public class PropertiesAttributeSerializer implements AttributeSerializer<Map<?, ?>> {
   public static final PropertiesAttributeSerializer PROPERTIES = new PropertiesAttributeSerializer("properties", "property", "name");
   private final String collectionElement;
   private final String itemElement;
   private final String nameAttribute;

   public PropertiesAttributeSerializer(Enum<?> collectionElement, Enum<?> itemElement, Enum<?> nameAttribute) {
      this(collectionElement.toString(), itemElement.toString(), nameAttribute.toString());
   }

   public PropertiesAttributeSerializer(String collectionElement, String itemElement, String nameAttribute) {
      this.collectionElement = collectionElement;
      this.itemElement = itemElement;
      this.nameAttribute = nameAttribute;
   }

   @Override
   public void serialize(ConfigurationWriter writer, String name, Map<?, ?> properties) {
      if (!properties.isEmpty()) {
         if (writer.hasFeature(ConfigurationFormatFeature.BARE_COLLECTIONS)) {
            for (Map.Entry<?, ?> entry : properties.entrySet()) {
               writer.writeStartElement(itemElement);
               writer.writeAttribute(nameAttribute, entry.getKey().toString());
               writer.writeCharacters(entry.getValue().toString());
               writer.writeEndElement();
            }
         } else {
            writer.writeStartMap(collectionElement);
            for (Map.Entry<?, ?> entry : properties.entrySet()) {
               writer.writeMapItem(itemElement, nameAttribute, entry.getKey().toString(), entry.getValue().toString());
            }
            writer.writeEndMap();
         }
      }
   }
}
