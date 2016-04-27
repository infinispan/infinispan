package org.infinispan.persistence.remote.configuration;

import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.SerializeUtils;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * RemoteStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class RemoteStoreConfigurationSerializer extends AbstractStoreSerializer implements ConfigurationSerializer<RemoteStoreConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, RemoteStoreConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.REMOTE_STORE);
      configuration.attributes().write(writer);

      writeCommonStoreSubAttributes(writer, configuration);

      writeAsyncExecutor(writer, configuration.asyncExecutorFactory());
      writeConnectionPool(writer, configuration.connectionPool());
      writeServers(writer, configuration.servers());

      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   private void writeAsyncExecutor(XMLExtendedStreamWriter writer, ExecutorFactoryConfiguration executorFactoryConfiguration) throws XMLStreamException {
      AttributeSet attributes = executorFactoryConfiguration.attributes();
      if(attributes.isModified()) {
         writer.writeStartElement(Element.ASYNC_TRANSPORT_EXECUTOR);
         attributes.write(writer, ExecutorFactoryConfiguration.EXECUTOR_FACTORY, Attribute.FACTORY);
         SerializeUtils.writeTypedProperties(writer, executorFactoryConfiguration.properties());
         writer.writeEndElement();
      }
   }

   private void writeConnectionPool(XMLExtendedStreamWriter writer, ConnectionPoolConfiguration connectionPool) throws XMLStreamException {
      writer.writeStartElement(Element.CONNECTION_POOL);
      writer.writeAttribute(Attribute.EXHAUSTED_ACTION, connectionPool.exhaustedAction().name());
      writer.writeAttribute(Attribute.MAX_ACTIVE, Integer.toString(connectionPool.maxActive()));
      writer.writeAttribute(Attribute.MAX_IDLE, Integer.toString(connectionPool.maxIdle()));
      writer.writeAttribute(Attribute.MIN_IDLE, Integer.toString(connectionPool.minIdle()));
      writer.writeAttribute(Attribute.MAX_TOTAL, Integer.toString(connectionPool.maxTotal()));
      writer.writeAttribute(Attribute.MIN_EVICTABLE_IDLE_TIME, Long.toString(connectionPool.minEvictableIdleTime()));
      writer.writeAttribute(Attribute.TIME_BETWEEN_EVICTION_RUNS, Long.toString(connectionPool.timeBetweenEvictionRuns()));
      writer.writeAttribute(Attribute.TEST_WHILE_IDLE, Boolean.toString(connectionPool.testWhileIdle()));
      writer.writeEndElement();
   }

   private void writeServers(XMLExtendedStreamWriter writer, List<RemoteServerConfiguration> servers) throws XMLStreamException {
      for(RemoteServerConfiguration server : servers) {
         writer.writeStartElement(Element.SERVER);
         writer.writeAttribute(Attribute.HOST, server.host());
         writer.writeAttribute(Attribute.PORT, Integer.toString(server.port()));
         writer.writeEndElement();
      }
   }



}
