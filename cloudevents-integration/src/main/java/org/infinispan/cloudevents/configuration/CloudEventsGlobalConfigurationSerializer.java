package org.infinispan.cloudevents.configuration;

import static org.infinispan.cloudevents.configuration.CloudEventsConfigurationParser.NAMESPACE;
import static org.infinispan.cloudevents.configuration.CloudEventsConfigurationParser.PREFIX;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

/**
 * A {@link ConfigurationSerializer} implementation for {@link CloudEventsGlobalConfiguration}.
 * <p>
 * The {@link CloudEventsGlobalConfiguration} is only to be used internally so this implementation is a no-op.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CloudEventsGlobalConfigurationSerializer implements ConfigurationSerializer<CloudEventsGlobalConfiguration> {
   @Override
   public void serialize(ConfigurationWriter writer, CloudEventsGlobalConfiguration configuration) {
      String xmlns = NAMESPACE + Version.getMajorMinor();
      writer.writeStartElement(PREFIX, xmlns, Element.CLOUDEVENTS.getLocalName());
      writer.writeNamespace(PREFIX, xmlns);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
