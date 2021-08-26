package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationSerializer;
import org.infinispan.persistence.jdbc.common.configuration.Element;

/**
 * TableJdbcStoreConfigurationSerializer.
 *
 * @author William Burns
 * @since 13.0
 */
public class TableJdbcStoreConfigurationSerializer extends AbstractJdbcStoreConfigurationSerializer implements ConfigurationSerializer<TableJdbcStoreConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, TableJdbcStoreConfiguration configuration) {
      writer.writeStartElement(Element.TABLE_JDBC_STORE);
      writer.writeDefaultNamespace(AbstractSchemaJdbcConfiguration.NAMESPACE + Version.getMajorMinor());
      writeJdbcStoreAttributes(writer, configuration);
      writeCommonStoreSubAttributes(writer, configuration);
      SqlSerializerUtil.writeSchemaElement(writer, configuration);
      writeJDBCStoreConnection(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }


}
