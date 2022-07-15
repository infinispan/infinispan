package org.infinispan.multimap.configuration;

import java.util.Collection;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

public class MultimapCacheManagerConfigurationSerializer implements ConfigurationSerializer<MultimapCacheManagerConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, MultimapCacheManagerConfiguration configuration) {
      writer.writeStartMap(Element.MULTIMAPS);
      writer.writeDefaultNamespace(MultimapConfigurationParser.NAMESPACE + Version.getMajorMinor());
      writeConfigurations(writer, configuration.multimaps().values());
      writer.writeEndMap();
   }

   private void writeConfigurations(ConfigurationWriter writer, Collection<EmbeddedMultimapConfiguration> configs) {
      for (EmbeddedMultimapConfiguration config : configs) {
         writer.writeMapItem(Element.MULTIMAP, Attribute.NAME, config.name());
         config.attributes().write(writer);
         writer.writeEndMapItem();
      }
   }
}
