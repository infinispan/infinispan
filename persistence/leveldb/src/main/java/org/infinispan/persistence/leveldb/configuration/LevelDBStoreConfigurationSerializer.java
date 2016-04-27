package org.infinispan.persistence.leveldb.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * LevelDBStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class LevelDBStoreConfigurationSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<LevelDBStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, LevelDBStoreConfiguration configuration) throws XMLStreamException {
      AttributeSet attributes = configuration.attributes();
      writer.writeStartElement(Element.LEVELDB_STORE);
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      if (attributes.attribute(LevelDBStoreConfiguration.COMPRESSION_TYPE).isModified()) {
         writer.writeStartElement(Element.COMPRESSION);
         attributes.write(writer, LevelDBStoreConfiguration.COMPRESSION_TYPE, Attribute.TYPE);
         writer.writeEndElement();
      }
      if (attributes.attribute(LevelDBStoreConfiguration.EXPIRED_LOCATION).isModified() || attributes.attribute(LevelDBStoreConfiguration.EXPIRY_QUEUE_SIZE).isModified()) {
         writer.writeStartElement(Element.EXPIRATION);
         attributes.write(writer, LevelDBStoreConfiguration.EXPIRED_LOCATION, Attribute.PATH);
         attributes.write(writer, LevelDBStoreConfiguration.EXPIRY_QUEUE_SIZE, Attribute.QUEUE_SIZE);
         writer.writeEndElement();
      }
      if (attributes.attribute(LevelDBStoreConfiguration.IMPLEMENTATION_TYPE).isModified()) {
         writer.writeStartElement(Element.IMPLEMENTATION);
         attributes.write(writer, LevelDBStoreConfiguration.IMPLEMENTATION_TYPE, Attribute.TYPE);
         writer.writeEndElement();
      }
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

}
