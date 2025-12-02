package org.infinispan.anchored.configuration;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

public class AnchoredKeysConfigurationSerializer
      implements ConfigurationSerializer<AnchoredKeysConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, AnchoredKeysConfiguration configuration) {
      writer.writeStartElement(Element.ANCHORED_KEYS);
      writer.writeDefaultNamespace(AnchoredKeysConfigurationParser.NAMESPACE);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
