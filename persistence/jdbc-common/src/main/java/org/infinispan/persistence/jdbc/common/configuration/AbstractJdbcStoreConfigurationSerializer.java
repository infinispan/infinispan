package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.configuration.serializing.SerializeUtils.writeOptional;

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
}
