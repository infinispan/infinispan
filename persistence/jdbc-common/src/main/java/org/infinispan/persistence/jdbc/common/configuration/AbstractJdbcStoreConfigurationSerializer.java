package org.infinispan.persistence.jdbc.common.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.AbstractStoreSerializer;

/**
 * AbstractJdbcStoreConfigurationSerializer.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public abstract class  AbstractJdbcStoreConfigurationSerializer extends AbstractStoreSerializer {
   protected void writeJdbcStoreAttributes(ConfigurationWriter writer, AbstractJdbcStoreConfiguration<?> configuration) {
      configuration.attributes().write(writer);
   }

   protected void writeJDBCStoreConnection(ConfigurationWriter writer, AbstractJdbcStoreConfiguration<?> configuration) {
      ConnectionFactoryConfiguration cfc = configuration.connectionFactory();
      if (cfc instanceof SimpleConnectionFactoryConfiguration) {
         writeAttributes(writer, Element.SIMPLE_CONNECTION, ((SimpleConnectionFactoryConfiguration) cfc).attributes());
      } else if (cfc instanceof PooledConnectionFactoryConfiguration) {
         writeAttributes(writer, Element.CONNECTION_POOL, ((PooledConnectionFactoryConfiguration) cfc).attributes());
      } else if (cfc instanceof ManagedConnectionFactoryConfiguration) {
         writeAttributes(writer, Element.DATA_SOURCE, ((ManagedConnectionFactoryConfiguration) cfc).attributes());
      } else if (cfc instanceof CDIConnectionFactoryConfiguration) {
         writeAttributes(writer, Element.CDI_DATA_SOURCE, ((CDIConnectionFactoryConfiguration) cfc).attributes());
      }
   }

   private void writeAttributes(ConfigurationWriter writer, Element element, AttributeSet attributes) {
      writer.writeStartElement(element);
      attributes.write(writer);
      writer.writeEndElement();
   }
}
