package org.infinispan.cloudevents.configuration;

import static org.infinispan.cloudevents.configuration.CloudEventsConfigurationParser.NAMESPACE;
import static org.infinispan.cloudevents.configuration.CloudEventsConfigurationParser.PREFIX;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

public class CloudEventsConfigurationSerializer
      implements ConfigurationSerializer<CloudEventsConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, CloudEventsConfiguration configuration)
         throws XMLStreamException {
      String xmlns = NAMESPACE + Version.getMajorMinor();
      writer.writeStartElement(PREFIX, xmlns, Element.CLOUDEVENTS_CACHE.getLocalName());
      writer.writeNamespace(PREFIX, xmlns);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
