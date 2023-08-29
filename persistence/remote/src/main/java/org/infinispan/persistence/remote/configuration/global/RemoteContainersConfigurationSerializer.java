package org.infinispan.persistence.remote.configuration.global;

import static org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationParser.NAMESPACE;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationParser.PREFIX;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.persistence.remote.configuration.Element;

public class RemoteContainersConfigurationSerializer
      implements ConfigurationSerializer<RemoteContainersConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, RemoteContainersConfiguration configuration) {
      if (!configuration.configurations().isEmpty()) {
         String xmlns = NAMESPACE + Version.getMajorMinor();
         writer.writeStartElement(PREFIX, xmlns, Element.REMOTE_CACHE_CONTAINERS);
         writer.writeNamespace(PREFIX, xmlns);
         configuration.configurations().values().forEach(c -> {
            writer.writeStartElement(PREFIX, xmlns, Element.REMOTE_CACHE_CONTAINER);
            c.attributes().write(writer);
            writer.writeEndElement();
         });
         writer.writeEndElement();
      }
   }
}
