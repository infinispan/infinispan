package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.configuration.serializing.SerializeUtils.writeOptional;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;

/**
 * AbstractJdbcStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public abstract class AbstractJdbcStoreConfigurationSerializer extends AbstractStoreSerializer {
   protected void writeJdbcStoreAttributes(ConfigurationWriter writer, AbstractJdbcStoreConfiguration configuration) {
      configuration.attributes().write(writer);
   }

   private void writeJDBCStoreConnection(ConfigurationWriter writer, SimpleConnectionFactoryConfiguration configuration) {
      writer.writeStartElement(Element.SIMPLE_CONNECTION);
      writeOptional(writer, Attribute.CONNECTION_URL, configuration.connectionUrl());
      writeOptional(writer, Attribute.DRIVER_CLASS, configuration.driverClass());
      writeOptional(writer, Attribute.USERNAME, configuration.username());
      writeOptional(writer, Attribute.PASSWORD, configuration.password());
      writer.writeEndElement();
   }

   private void writeJDBCStoreConnection(ConfigurationWriter writer, PooledConnectionFactoryConfiguration configuration) {
      writer.writeStartElement(Element.CONNECTION_POOL);
      writeOptional(writer, Attribute.CONNECTION_URL, configuration.connectionUrl());
      writeOptional(writer, Attribute.DRIVER_CLASS, configuration.driverClass());
      writeOptional(writer, Attribute.USERNAME, configuration.username());
      writeOptional(writer, Attribute.PASSWORD, configuration.password());
      writer.writeEndElement();
   }

   private void writeJDBCStoreConnection(ConfigurationWriter writer, ManagedConnectionFactoryConfiguration configuration) {
      writer.writeStartElement(Element.DATA_SOURCE);
      writer.writeAttribute(Attribute.JNDI_URL, configuration.jndiUrl());
      writer.writeEndElement();
   }

   protected void writeJDBCStoreConnection(ConfigurationWriter writer, AbstractJdbcStoreConfiguration configuration) {
      ConnectionFactoryConfiguration cfc = configuration.connectionFactory();
      if (cfc instanceof SimpleConnectionFactoryConfiguration) {
         writeJDBCStoreConnection(writer, (SimpleConnectionFactoryConfiguration) cfc);
      } else if (cfc instanceof PooledConnectionFactoryConfiguration) {
         writeJDBCStoreConnection(writer, (PooledConnectionFactoryConfiguration) cfc);
      } else if (cfc instanceof ManagedConnectionFactoryConfiguration) {
         writeJDBCStoreConnection(writer, (ManagedConnectionFactoryConfiguration) cfc);
      }
   }

   protected void writeJDBCStoreTable(ConfigurationWriter writer, Element element, TableManipulationConfiguration configuration) {
      AttributeSet attributes = configuration.attributes();
      writer.writeStartElement(element);
      attributes.write(writer, TableManipulationConfiguration.TABLE_NAME_PREFIX, Attribute.PREFIX);
      attributes.write(writer, TableManipulationConfiguration.BATCH_SIZE, Attribute.BATCH_SIZE);
      attributes.write(writer, TableManipulationConfiguration.FETCH_SIZE, Attribute.FETCH_SIZE);
      attributes.write(writer, TableManipulationConfiguration.CREATE_ON_START, Attribute.CREATE_ON_START);
      attributes.write(writer, TableManipulationConfiguration.DROP_ON_EXIT, Attribute.DROP_ON_EXIT);

      writeJDBCStoreColumn(writer, Element.ID_COLUMN, configuration.idColumnConfiguration().attributes(), IdColumnConfiguration.ID_COLUMN_NAME, IdColumnConfiguration.ID_COLUMN_TYPE);
      writeJDBCStoreColumn(writer, Element.DATA_COLUMN, configuration.dataColumnConfiguration().attributes(), DataColumnConfiguration.DATA_COLUMN_NAME, DataColumnConfiguration.DATA_COLUMN_TYPE);
      writeJDBCStoreColumn(writer, Element.TIMESTAMP_COLUMN, configuration.timeStampColumnConfiguration().attributes(), TimestampColumnConfiguration.TIMESTAMP_COLUMN_NAME, TimestampColumnConfiguration.TIMESTAMP_COLUMN_TYPE);

      writeJDBCStoreColumn(writer, Element.SEGMENT_COLUMN, configuration.segmentColumnConfiguration().attributes(), SegmentColumnConfiguration.SEGMENT_COLUMN_NAME, SegmentColumnConfiguration.SEGMENT_COLUMN_TYPE);

      writer.writeEndElement();
   }

   private void writeJDBCStoreColumn(ConfigurationWriter writer, Element element, AttributeSet attributes, AttributeDefinition<?> columnName,
                                     AttributeDefinition<?> columnType) {
      writer.writeStartElement(element);
      attributes.write(writer, columnName, Attribute.NAME);
      attributes.write(writer, columnType, Attribute.TYPE);
      writer.writeEndElement();
   }
}
