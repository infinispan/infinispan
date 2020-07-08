package org.infinispan.persistence.rocksdb.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Version;
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
      writer.writeDefaultNamespace(RocksDBStoreConfigurationParser.NAMESPACE + Version.getMajorMinor());
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      if (attributes.attribute(RocksDBStoreConfiguration.COMPRESSION_TYPE).isModified()) {
         writer.writeStartElement(Element.COMPRESSION);
         attributes.write(writer, RocksDBStoreConfiguration.COMPRESSION_TYPE, Attribute.TYPE);
         writer.writeEndElement();
      }
      RocksDBExpirationConfiguration expiration = configuration.expiration();
      AttributeSet expirationAttrs = expiration.attributes();
      if (expirationAttrs.attribute(RocksDBExpirationConfiguration.EXPIRED_LOCATION).isModified() || expirationAttrs.attribute(RocksDBExpirationConfiguration.EXPIRY_QUEUE_SIZE).isModified()) {
         writer.writeStartElement(Element.EXPIRATION);
         expirationAttrs.write(writer, RocksDBExpirationConfiguration.EXPIRED_LOCATION, Attribute.PATH);
         expirationAttrs.write(writer, RocksDBExpirationConfiguration.EXPIRY_QUEUE_SIZE, Attribute.QUEUE_SIZE);
         writer.writeEndElement();
      }
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

}
