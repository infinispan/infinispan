package org.infinispan.persistence.jpa.configuration;

import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

import javax.xml.stream.XMLStreamException;

/**
 * JpaStoreConfigurationSerializer.
 *
 * @since 9.0
 * @author Tristan Tarrant
 */
public class JpaStoreConfigurationSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<JpaStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, JpaStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.JPA_STORE);
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

}
