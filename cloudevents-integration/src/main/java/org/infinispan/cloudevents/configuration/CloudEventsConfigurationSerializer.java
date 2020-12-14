package org.infinispan.cloudevents.configuration;

import static org.infinispan.cloudevents.configuration.CloudEventsConfigurationParser.NAMESPACE;
import static org.infinispan.cloudevents.configuration.CloudEventsConfigurationParser.PREFIX;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

public class CloudEventsConfigurationSerializer
      implements ConfigurationSerializer<CloudEventsConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, CloudEventsConfiguration configuration) {
      String xmlns = NAMESPACE + Version.getMajorMinor();
      writer.writeStartElement(PREFIX, xmlns, Element.CLOUDEVENTS_CACHE.getLocalName());
      writer.writeNamespace(PREFIX, xmlns);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
