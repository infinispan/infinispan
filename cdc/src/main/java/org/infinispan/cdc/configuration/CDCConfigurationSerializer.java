package org.infinispan.cdc.configuration;


import static org.infinispan.cdc.configuration.CDCConfigurationParser.NAMESPACE;

import java.util.Properties;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationSerializer;

public final class CDCConfigurationSerializer extends AbstractJdbcStoreConfigurationSerializer implements ConfigurationSerializer<ChangeDataCaptureConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, ChangeDataCaptureConfiguration configuration) {
      String xmlns = NAMESPACE + Version.getMajorMinor();
      writer.writeStartElement(Element.CDC);
      writer.writeNamespace("", xmlns);
      writeCDCAttributes(writer, configuration);
      writeJDBCStoreConnection(writer, configuration);
      writeTableElement(writer, configuration.table());
      writeConnectorProperties(writer, configuration.connectorProperties());
      writer.writeEndElement();
   }

   private void writeCDCAttributes(ConfigurationWriter writer, ChangeDataCaptureConfiguration configuration) {
      AttributeSet attributes = configuration.attributes();
      attributes.write(writer, ChangeDataCaptureConfiguration.ENABLED, Attribute.ENABLED);
      attributes.write(writer, ChangeDataCaptureConfiguration.FOREIGN_KEYS, Attribute.FOREIGN_KEYS);
   }

   private void writeTableElement(ConfigurationWriter writer, TableConfiguration configuration) {
      if (configuration.attributes().isModified()) {
         writer.writeStartElement(Element.TABLE);
         configuration.attributes().write(writer);
         writePrimaryKey(writer, configuration.primaryKey());
         configuration.columns().forEach(c -> writeColumnElement(writer, c));
         writer.writeEndElement();
      }
   }

   private void writePrimaryKey(ConfigurationWriter writer, ColumnConfiguration configuration) {
      if (configuration.attributes().isModified()) {
         writer.writeStartElement(Element.PRIMARY_KEY);
         configuration.attributes().write(writer);
         writer.writeEndElement();
      }
   }

   private void writeColumnElement(ConfigurationWriter writer, ColumnConfiguration configuration) {
      if (configuration.attributes().isModified()) {
         writer.writeStartElement(Element.COLUMN);
         configuration.attributes().write(writer);
         writer.writeEndElement();
      }
   }

   private void writeConnectorProperties(ConfigurationWriter writer, Properties properties) {
      if (!properties.isEmpty()) {
         writer.writeStartElement(Element.CONNECTOR_PROPERTIES);

         properties.forEach((k, v) -> {
            writer.writeStartElement((String) k);
            writer.writeCharacters((String) v);
            writer.writeEndElement();
         });

         writer.writeEndElement();
      }
   }
}
