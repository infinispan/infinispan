package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.persistence.jdbc.common.DatabaseType;

public abstract class AbstractJdbcStoreConfigurationParser implements ConfigurationParser {
   protected boolean handleCommonAttributes(ConfigurationReader reader,
         AbstractJdbcStoreConfigurationBuilder<?, ?> builder, Attribute attribute, String value) {
      switch (attribute) {
         case DIALECT:
            builder.dialect(DatabaseType.valueOf(value));
            break;
         case DB_MAJOR_VERSION:
         case DB_MINOR_VERSION:
            CONFIG.configDeprecated(attribute);
            break;
         case READ_QUERY_TIMEOUT:
            builder.readQueryTimeout(Integer.parseInt(value));
            break;
         case WRITE_QUERY_TIMEOUT:
            builder.writeQueryTimeout(Integer.parseInt(value));
            break;
         default:
            return false;
      }
      return true;
   }

   protected boolean handleCommonElement(AbstractJdbcStoreConfigurationBuilder<?, ?> builder, Element element,
         ConfigurationReader reader) {
      switch (element) {
         case CONNECTION_POOL: {
            parseConnectionPoolAttributes(reader, builder.connectionPool());
            break;
         }
         case DATA_SOURCE: {
            parseDataSourceAttributes(reader, builder.dataSource());
            break;
         }
         case CDI_DATA_SOURCE: {
            parseCDIDataSourceAttributes(reader, builder.cdiDataSource());
            break;
         }
         case SIMPLE_CONNECTION: {
            parseSimpleConnectionAttributes(reader, builder.simpleConnection());
            break;
         }
         default:
            return false;
      }
      return true;
   }

   protected void parseDataSourceAttributes(ConfigurationReader reader,
         ManagedConnectionFactoryConfigurationBuilder<?> builder) {
      String jndiUrl = ParseUtils.requireSingleAttribute(reader, Attribute.JNDI_URL.getLocalName());
      builder.jndiUrl(jndiUrl);
      ParseUtils.requireNoContent(reader);
   }

   protected void parseCDIDataSourceAttributes(ConfigurationReader reader,
                                               CDIConnectionFactoryConfigurationBuilder<?> builder) {
      ParseUtils.parseAttributes(reader, builder);
      ParseUtils.requireNoContent(reader);
   }

   protected void parseConnectionPoolAttributes(ConfigurationReader reader,
         PooledConnectionFactoryConfigurationBuilder<?> builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case PROPERTIES_FILE: {
               builder.propertyFile(value);
               break;
            }
            case CONNECTION_URL: {
               builder.connectionUrl(value);
               break;
            }
            case DRIVER: {
               builder.driverClass(value);
               break;
            }
            case PASSWORD: {
               builder.password(value);
               break;
            }
            case USERNAME: {
               builder.username(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   protected void parseSimpleConnectionAttributes(ConfigurationReader reader,
         SimpleConnectionFactoryConfigurationBuilder<?> builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CONNECTION_URL: {
               builder.connectionUrl(value);
               break;
            }
            case DRIVER: {
               builder.driverClass(value);
               break;
            }
            case PASSWORD: {
               builder.password(value);
               break;
            }
            case USERNAME: {
               builder.username(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }
}
