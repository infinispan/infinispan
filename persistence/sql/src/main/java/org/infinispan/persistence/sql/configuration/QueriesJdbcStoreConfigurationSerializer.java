package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationSerializer;
import org.infinispan.persistence.jdbc.common.configuration.Element;

/**
 * QueriesJdbcStoreConfigurationSerializer.
 *
 * @author William Burns
 * @since 13.0
 */
public class QueriesJdbcStoreConfigurationSerializer extends AbstractJdbcStoreConfigurationSerializer implements ConfigurationSerializer<QueriesJdbcStoreConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, QueriesJdbcStoreConfiguration configuration) {
      writer.writeStartElement(Element.QUERY_JDBC_STORE);
      writer.writeDefaultNamespace(AbstractSchemaJdbcConfiguration.NAMESPACE + Version.getMajorMinor());
      writeJdbcStoreAttributes(writer, configuration);
      writeCommonStoreSubAttributes(writer, configuration);
      SqlSerializerUtil.writeSchemaElement(writer, configuration);
      writeQueryElements(writer, configuration);
      writeJDBCStoreConnection(writer, configuration);
      writeCommonStoreElements(writer, configuration);
      writer.writeEndElement();
   }

   static void writeQueryElements(ConfigurationWriter writer, QueriesJdbcStoreConfiguration configuration) {
      QueriesJdbcConfiguration queryConfig = configuration.getQueriesJdbcConfiguration();
      if (queryConfig.attributes().isModified()) {
         writer.writeStartElement(Element.QUERIES);
         queryConfig.attributes().write(writer);
         writer.writeEndElement();
      }
   }
}
