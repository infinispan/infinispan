package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;
import static org.infinispan.persistence.jdbc.configuration.Element.MIXED_KEYED_JDBC_STORE;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.kohsuke.MetaInfServices;

/**
 * JDBC cache store configuration parser.
 *
 * @author Galder Zamarreño
 * @since 9.0
 */
@MetaInfServices
@Namespaces({
   @Namespace(root = "string-keyed-jdbc-store"),
   @Namespace(root = "binary-keyed-jdbc-store"),
   @Namespace(root = "mixed-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:9.1", root = "string-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:9.0", root = "string-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:9.0", root = "binary-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:9.0", root = "mixed-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:8.0", root = "string-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:8.0", root = "binary-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:8.0", root = "mixed-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.0", root = "string-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.1", root = "string-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.2", root = "string-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.0", root = "binary-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.1", root = "binary-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.2", root = "binary-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.0", root = "mixed-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.1", root = "mixed-keyed-jdbc-store"),
   @Namespace(uri = "urn:infinispan:config:store:jdbc:7.2", root = "mixed-keyed-jdbc-store"),
})
public class JdbcStoreConfigurationParser implements ConfigurationParser {

   public JdbcStoreConfigurationParser() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case STRING_KEYED_JDBC_STORE: {
            parseStringKeyedJdbcStore(reader, builder.persistence());
            break;
         }
         case BINARY_KEYED_JDBC_STORE:
         case MIXED_KEYED_JDBC_STORE: {
            throw new CacheConfigurationException("Binary and Mixed Keyed JDBC stores were removed in 9.0. " +
                  "Please use JdbcStringBasedStore instead");
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseStringKeyedJdbcStore(final XMLExtendedStreamReader reader,
                                          PersistenceConfigurationBuilder persistenceBuilder) throws XMLStreamException {
      JdbcStringBasedStoreConfigurationBuilder builder = new JdbcStringBasedStoreConfigurationBuilder(
            persistenceBuilder);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case KEY_TO_STRING_MAPPER:
               builder.key2StringMapper(value);
               break;
            case DIALECT:
               builder.dialect(DatabaseType.valueOf(value));
               break;
            case DB_MAJOR_VERSION:
               builder.dbMajorVersion(Integer.parseInt(value));
               break;
            case DB_MINOR_VERSION:
               builder.dbMinorVersion(Integer.parseInt(value));
               break;
            default:
               Parser.parseStoreAttribute(reader, i, builder);
               break;
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case STRING_KEYED_TABLE: {
               parseTable(reader, builder.table());
               break;
            }
            case CONNECTION_POOL: {
               parseConnectionPoolAttributes(reader, builder.connectionPool());
               break;
            }
            case DATA_SOURCE: {
               parseDataSourceAttributes(reader, builder.dataSource());
               break;
            }
            case SIMPLE_CONNECTION: {
               parseSimpleConnectionAttributes(reader, builder.simpleConnection());
               break;
            }
            default: {
               Parser.parseStoreElement(reader, builder);
               break;
            }
         }
      }
      persistenceBuilder.addStore(builder);
   }

   private void parseDataSourceAttributes(XMLExtendedStreamReader reader,
                                          ManagedConnectionFactoryConfigurationBuilder<?> builder) throws XMLStreamException {
      String jndiUrl = ParseUtils.requireSingleAttribute(reader, Attribute.JNDI_URL.getLocalName());
      builder.jndiUrl(jndiUrl);
      ParseUtils.requireNoContent(reader);
   }

   private void parseConnectionPoolAttributes(XMLExtendedStreamReader reader,
                                              PooledConnectionFactoryConfigurationBuilder<?> builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PROPERTIES_FILE: {
               builder.propertyFile(value);
               break;
            }
            case CONNECTION_URL: {
               builder.connectionUrl(value);
               break;
            }
            case DRIVER_CLASS: {
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

   private void parseSimpleConnectionAttributes(XMLExtendedStreamReader reader,
                                                SimpleConnectionFactoryConfigurationBuilder<?> builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case CONNECTION_URL: {
               builder.connectionUrl(value);
               break;
            }
            case DRIVER_CLASS: {
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

   private void parseTable(XMLExtendedStreamReader reader, TableManipulationConfigurationBuilder<?, ?> builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case BATCH_SIZE: {
               builder.batchSize(Integer.parseInt(value));
               break;
            }
            case CREATE_ON_START: {
               builder.createOnStart(Boolean.parseBoolean(value));
               break;
            }
            case DROP_ON_EXIT: {
               builder.dropOnExit(Boolean.parseBoolean(value));
               break;
            }
            case FETCH_SIZE: {
               builder.fetchSize(Integer.parseInt(value));
               break;
            }
            case PREFIX: {
               builder.tableNamePrefix(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      parseTableElements(reader, builder);
   }

   private void parseTableElements(XMLExtendedStreamReader reader, TableManipulationConfigurationBuilder<?, ?> builder)
         throws XMLStreamException {
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case ID_COLUMN: {
               Column column = parseTableElementAttributes(reader);
               builder.idColumnName(column.name);
               builder.idColumnType(column.type);
               break;
            }
            case DATA_COLUMN: {
               Column column = parseTableElementAttributes(reader);
               builder.dataColumnName(column.name);
               builder.dataColumnType(column.type);
               break;
            }
            case TIMESTAMP_COLUMN: {
               Column column = parseTableElementAttributes(reader);
               builder.timestampColumnName(column.name);
               builder.timestampColumnType(column.type);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
   }

   private Column parseTableElementAttributes(XMLExtendedStreamReader reader) throws XMLStreamException {
      Column column = new Column();
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME: {
               column.name = value;
               break;
            }
            case TYPE: {
               column.type = value;
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
      return column;
   }

   class Column {
      String name;
      String type;
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

}
