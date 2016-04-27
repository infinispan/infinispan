package org.infinispan.persistence.sifs.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

import javax.xml.stream.XMLStreamException;

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
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeDataElement(writer, configuration);
      writeIndexElement(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeDataElement(XMLExtendedStreamWriter writer, SoftIndexFileStoreConfiguration configuration) throws XMLStreamException {
      configuration.attributes().write(writer, Element.DATA.getLocalName(),
            SoftIndexFileStoreConfiguration.DATA_LOCATION,
            SoftIndexFileStoreConfiguration.MAX_FILE_SIZE,
            SoftIndexFileStoreConfiguration.SYNC_WRITES);
   }

   private void writeIndexElement(XMLExtendedStreamWriter writer, SoftIndexFileStoreConfiguration configuration) throws XMLStreamException {
      configuration.attributes().write(writer, Element.INDEX.getLocalName(),
            SoftIndexFileStoreConfiguration.INDEX_LOCATION,
            SoftIndexFileStoreConfiguration.INDEX_QUEUE_LENGTH,
            SoftIndexFileStoreConfiguration.INDEX_SEGMENTS,
            SoftIndexFileStoreConfiguration.MIN_NODE_SIZE,
            SoftIndexFileStoreConfiguration.MAX_NODE_SIZE);
   }
}
