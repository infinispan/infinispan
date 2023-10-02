package org.infinispan.persistence.jdbc.common.configuration;

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
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }

   private void writeJDBCStoreConnection(ConfigurationWriter writer, PooledConnectionFactoryConfiguration configuration) {
      writer.writeStartElement(Element.CONNECTION_POOL);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }

   private void writeJDBCStoreConnection(ConfigurationWriter writer, ManagedConnectionFactoryConfiguration configuration) {
      writer.writeStartElement(Element.DATA_SOURCE);
      configuration.attributes().write(writer);
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
}
