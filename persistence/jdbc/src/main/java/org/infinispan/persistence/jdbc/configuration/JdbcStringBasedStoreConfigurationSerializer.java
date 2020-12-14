package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

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


}
