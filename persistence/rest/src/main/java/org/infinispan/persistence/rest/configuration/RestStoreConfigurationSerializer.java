package org.infinispan.persistence.rest.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * RestStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class RestStoreConfigurationSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<RestStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, RestStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.REST_STORE);
      configuration.attributes().write(writer);
      writeCommonStoreSubAttributes(writer, configuration);
      writeServer(writer, configuration);
      writeConnectionPool(writer, configuration.connectionPool());
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeConnectionPool(XMLExtendedStreamWriter writer, ConnectionPoolConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.CONNECTION_POOL);
      writer.writeAttribute(Attribute.BUFFER_SIZE, Integer.toString(configuration.bufferSize()));
      writer.writeAttribute(Attribute.MAX_CONNECTIONS_PER_HOST, Integer.toString(configuration.maxConnectionsPerHost()));
      writer.writeAttribute(Attribute.MAX_TOTAL_CONNECTIONS, Integer.toString(configuration.maxTotalConnections()));
      writer.writeAttribute(Attribute.CONNECTION_TIMEOUT, Integer.toString(configuration.connectionTimeout()));
      writer.writeAttribute(Attribute.SOCKET_TIMEOUT, Integer.toString(configuration.socketTimeout()));
      writer.writeAttribute(Attribute.TCP_NO_DELAY, Boolean.toString(configuration.tcpNoDelay()));
      writer.writeEndElement();
   }

   private void writeServer(XMLExtendedStreamWriter writer, RestStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.SERVER);
      writer.writeAttribute(Attribute.HOST, configuration.host());
      writer.writeAttribute(Attribute.PORT, Integer.toString(configuration.port()));
      writer.writeEndElement();
   }


}
