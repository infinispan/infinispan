package org.infinispan.persistence.jdbc.configuration;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;
import org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory;

import static org.infinispan.configuration.serializing.SerializeUtils.writeOptional;

/**
 * AbstractJdbcStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public abstract class AbstractJdbcStoreConfigurationSerializer extends AbstractStoreSerializer {
   protected void writeJdbcStoreAttributes(XMLExtendedStreamWriter writer, AbstractJdbcStoreConfiguration configuration) throws XMLStreamException {
      configuration.attributes().write(writer);
   }

   private void writeJDBCStoreConnection(XMLExtendedStreamWriter writer, SimpleConnectionFactoryConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.SIMPLE_CONNECTION);
      writeOptional(writer, Attribute.CONNECTION_URL, configuration.connectionUrl());
      writeOptional(writer, Attribute.DRIVER_CLASS, configuration.driverClass());
      writeOptional(writer, Attribute.USERNAME, configuration.username());
      writeOptional(writer, Attribute.PASSWORD, configuration.password());
      writer.writeEndElement();
   }

   private void writeJDBCStoreConnection(XMLExtendedStreamWriter writer, PooledConnectionFactoryConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.CONNECTION_POOL);
      writeOptional(writer, Attribute.CONNECTION_URL, configuration.connectionUrl());
      writeOptional(writer, Attribute.DRIVER_CLASS, configuration.driverClass());
      writeOptional(writer, Attribute.USERNAME, configuration.username());
      writeOptional(writer, Attribute.PASSWORD, configuration.password());
      writer.writeEndElement();
   }

   private void writeJDBCStoreConnection(XMLExtendedStreamWriter writer, ManagedConnectionFactoryConfiguration configuration) throws XMLStreamException {
      writer.writeStartElement(Element.DATA_SOURCE);
      writer.writeAttribute(Attribute.JNDI_URL, configuration.jndiUrl());
      writer.writeEndElement();
   }

   protected void writeJDBCStoreConnection(XMLExtendedStreamWriter writer, AbstractJdbcStoreConfiguration configuration) throws XMLStreamException {
      ConnectionFactoryConfiguration cfc = configuration.connectionFactory();
      if (cfc instanceof SimpleConnectionFactoryConfiguration) {
         writeJDBCStoreConnection(writer, (SimpleConnectionFactoryConfiguration) cfc);
      } else if (cfc instanceof PooledConnectionFactoryConfiguration) {
         writeJDBCStoreConnection(writer, (PooledConnectionFactoryConfiguration) cfc);
      } else if (cfc instanceof ManagedConnectionFactoryConfiguration) {
         writeJDBCStoreConnection(writer, (ManagedConnectionFactoryConfiguration) cfc);
      }
   }

   protected void writeJDBCStoreTable(XMLExtendedStreamWriter writer, Element element, TableManipulationConfiguration configuration) throws XMLStreamException {
      AttributeSet attributes = configuration.attributes();
      writer.writeStartElement(element);
      attributes.write(writer, TableManipulationConfiguration.TABLE_NAME_PREFIX, Attribute.PREFIX);
      attributes.write(writer, TableManipulationConfiguration.BATCH_SIZE, Attribute.BATCH_SIZE);
      attributes.write(writer, TableManipulationConfiguration.FETCH_SIZE, Attribute.FETCH_SIZE);
      attributes.write(writer, TableManipulationConfiguration.CREATE_ON_START, Attribute.CREATE_ON_START);
      attributes.write(writer, TableManipulationConfiguration.DROP_ON_EXIT, Attribute.DROP_ON_EXIT);

      writeJDBCStoreColumn(writer, Element.ID_COLUMN, attributes, TableManipulationConfiguration.ID_COLUMN_NAME, TableManipulationConfiguration.ID_COLUMN_TYPE);
      writeJDBCStoreColumn(writer, Element.DATA_COLUMN, attributes, TableManipulationConfiguration.DATA_COLUMN_NAME, TableManipulationConfiguration.DATA_COLUMN_TYPE);
      writeJDBCStoreColumn(writer, Element.TIMESTAMP_COLUMN, attributes, TableManipulationConfiguration.TIMESTAMP_COLUMN_NAME, TableManipulationConfiguration.TIMESTAMP_COLUMN_TYPE);

      writer.writeEndElement();
   }

   private void writeJDBCStoreColumn(XMLExtendedStreamWriter writer, Element element, AttributeSet attributes, AttributeDefinition<?> columnName,
         AttributeDefinition<?> columnType) throws XMLStreamException {
      writer.writeStartElement(element);
      attributes.write(writer, columnName, Attribute.NAME);
      attributes.write(writer, columnType, Attribute.TYPE);
      writer.writeEndElement();
   }
}
