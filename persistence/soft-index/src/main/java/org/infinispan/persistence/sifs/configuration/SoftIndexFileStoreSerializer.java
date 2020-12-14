package org.infinispan.persistence.sifs.configuration;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

/**
 * SoftIndexFileStoreSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class SoftIndexFileStoreSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<SoftIndexFileStoreConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, SoftIndexFileStoreConfiguration configuration) {
      writer.writeStartElement(Element.SOFT_INDEX_FILE_STORE);
      writer.writeDefaultNamespace(SoftIndexFileStoreConfigurationParser.NAMESPACE + Version.getMajorMinor());
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeDataElement(writer, configuration);
      writeIndexElement(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeDataElement(ConfigurationWriter writer, SoftIndexFileStoreConfiguration configuration) {
      configuration.data().attributes().write(writer, Element.DATA.getLocalName(),
            DataConfiguration.DATA_LOCATION,
            DataConfiguration.MAX_FILE_SIZE,
            DataConfiguration.SYNC_WRITES);
   }

   private void writeIndexElement(ConfigurationWriter writer, SoftIndexFileStoreConfiguration configuration) {
      configuration.index().attributes().write(writer, Element.INDEX.getLocalName(),
            IndexConfiguration.INDEX_LOCATION,
            IndexConfiguration.INDEX_QUEUE_LENGTH,
            IndexConfiguration.INDEX_SEGMENTS,
            IndexConfiguration.MIN_NODE_SIZE,
            IndexConfiguration.MAX_NODE_SIZE);
   }
}
