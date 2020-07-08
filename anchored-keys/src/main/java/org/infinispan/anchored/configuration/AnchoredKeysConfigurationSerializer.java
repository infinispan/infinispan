package org.infinispan.anchored.configuration;

import static org.infinispan.anchored.configuration.AnchoredKeysConfigurationParser.NAMESPACE;
import static org.infinispan.anchored.configuration.AnchoredKeysConfigurationParser.PREFIX;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

public class AnchoredKeysConfigurationSerializer
      implements ConfigurationSerializer<AnchoredKeysConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, AnchoredKeysConfiguration configuration)
         throws XMLStreamException {
      String xmlns = NAMESPACE + Version.getMajorMinor();
      writer.writeStartElement(PREFIX, xmlns, Element.ANCHORED_KEYS.getLocalName());
      writer.writeNamespace(PREFIX, xmlns);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
