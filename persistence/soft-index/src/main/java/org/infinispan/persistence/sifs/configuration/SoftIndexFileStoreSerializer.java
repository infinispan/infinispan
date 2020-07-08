package org.infinispan.persistence.sifs.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * SoftIndexFileStoreSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class SoftIndexFileStoreSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<SoftIndexFileStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, SoftIndexFileStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.SOFT_INDEX_FILE_STORE);
      writer.writeDefaultNamespace(SoftIndexFileStoreConfigurationParser.NAMESPACE + Version.getMajorMinor());
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeDataElement(writer, configuration);
      writeIndexElement(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeDataElement(XMLExtendedStreamWriter writer, SoftIndexFileStoreConfiguration configuration) throws XMLStreamException {
      configuration.data().attributes().write(writer, Element.DATA.getLocalName(),
            DataConfiguration.DATA_LOCATION,
            DataConfiguration.MAX_FILE_SIZE,
            DataConfiguration.SYNC_WRITES);
   }

   private void writeIndexElement(XMLExtendedStreamWriter writer, SoftIndexFileStoreConfiguration configuration) throws XMLStreamException {
      configuration.index().attributes().write(writer, Element.INDEX.getLocalName(),
            IndexConfiguration.INDEX_LOCATION,
            IndexConfiguration.INDEX_QUEUE_LENGTH,
            IndexConfiguration.INDEX_SEGMENTS,
            IndexConfiguration.MIN_NODE_SIZE,
            IndexConfiguration.MAX_NODE_SIZE);
   }
}
