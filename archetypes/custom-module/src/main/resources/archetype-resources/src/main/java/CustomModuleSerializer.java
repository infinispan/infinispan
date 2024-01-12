package ${package};

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

public class CustomModuleSerializer implements ConfigurationSerializer<CustomModuleConfiguration> {
   @Override
   public void serialize(ConfigurationWriter writer, CustomModuleConfiguration configuration) {
      writer.writeStartElement(Element.ROOT);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
