package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationSerializer;
import org.infinispan.persistence.jdbc.common.configuration.Attribute;
import org.infinispan.persistence.jdbc.common.configuration.Element;

/**
 * JdbcStringBasedStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class JdbcStringBasedStoreConfigurationSerializer extends AbstractJdbcStoreConfigurationSerializer implements ConfigurationSerializer<JdbcStringBasedStoreConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, JdbcStringBasedStoreConfiguration configuration) {
      writer.writeStartElement(Element.STRING_KEYED_JDBC_STORE);
      writer.writeDefaultNamespace(JdbcStoreConfigurationParser.NAMESPACE + Version.getMajorMinor());
      writeJdbcStoreAttributes(writer, configuration);
      writeCommonStoreSubAttributes(writer, configuration);
      writeJDBCStoreTable(writer, Element.STRING_KEYED_TABLE, configuration.table());
      writeJDBCStoreConnection(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
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
