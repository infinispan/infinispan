package org.infinispan.anchored.configuration;

import static org.infinispan.anchored.configuration.AnchoredKeysConfigurationParser.NAMESPACE;
import static org.infinispan.anchored.configuration.AnchoredKeysConfigurationParser.PREFIX;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

public class AnchoredKeysConfigurationSerializer
      implements ConfigurationSerializer<AnchoredKeysConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, AnchoredKeysConfiguration configuration) {
      String xmlns = NAMESPACE + Version.getMajorMinor();
      writer.writeStartElement(PREFIX, xmlns, Element.ANCHORED_KEYS.getLocalName());
      writer.writeNamespace(PREFIX, xmlns);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
