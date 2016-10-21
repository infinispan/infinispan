package org.infinispan.persistence.rocksdb.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * RocksDBStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class RocksDBStoreConfigurationSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<RocksDBStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, RocksDBStoreConfiguration configuration) throws XMLStreamException {
      AttributeSet attributes = configuration.attributes();
      writer.writeStartElement(Element.ROCKSDB_STORE);
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      if (attributes.attribute(RocksDBStoreConfiguration.COMPRESSION_TYPE).isModified()) {
         writer.writeStartElement(Element.COMPRESSION);
         attributes.write(writer, RocksDBStoreConfiguration.COMPRESSION_TYPE, Attribute.TYPE);
         writer.writeEndElement();
      }
      if (attributes.attribute(RocksDBStoreConfiguration.EXPIRED_LOCATION).isModified() || attributes.attribute(RocksDBStoreConfiguration.EXPIRY_QUEUE_SIZE).isModified()) {
         writer.writeStartElement(Element.EXPIRATION);
         attributes.write(writer, RocksDBStoreConfiguration.EXPIRED_LOCATION, Attribute.PATH);
         attributes.write(writer, RocksDBStoreConfiguration.EXPIRY_QUEUE_SIZE, Attribute.QUEUE_SIZE);
         writer.writeEndElement();
      }
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

}
