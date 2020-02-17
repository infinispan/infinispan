package org.infinispan.anchored.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 *
 */
public class AnchoredKeysConfigurationSerializer
      implements ConfigurationSerializer<AnchoredKeysConfiguration> {
   @Override
   public void serialize(XMLExtendedStreamWriter writer, AnchoredKeysConfiguration configuration)
         throws XMLStreamException {
      writer.writeStartElement(Element.ANCHORED_KEYS.getLocalName());
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
