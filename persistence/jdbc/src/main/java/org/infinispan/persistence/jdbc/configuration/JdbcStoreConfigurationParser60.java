package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import java.util.Properties;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 *
 * JdbcStoreConfigurationParser60.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:jdbc:6.0", root = "stringKeyedJdbcStore"),
   @Namespace(root = "stringKeyedJdbcStore"),
   @Namespace(uri = "urn:infinispan:config:jdbc:6.0", root = "binaryKeyedJdbcStore"),
   @Namespace(root = "binaryKeyedJdbcStore"),
   @Namespace(uri = "urn:infinispan:config:jdbc:6.0", root = "mixedKeyedJdbcStore"),
   @Namespace(root = "mixedKeyedJdbcStore"),
})
public class JdbcStoreConfigurationParser60 implements ConfigurationParser {

   public JdbcStoreConfigurationParser60() {
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
      case BINARY_KEYED_JDBC_STORE: {
         parseBinaryKeyedJdbcStore(reader, builder.persistence());
         break;
      }
      case MIXED_KEYED_JDBC_STORE: {
         parseMixedKeyedJdbcStore(reader, builder.persistence());
         break;
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
         default:
            parseCommonStoreAttributes(reader, i, builder);
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
         default: {
            parseCommonJdbcStoreElements(reader, element, builder);
            break;
         }
         }
      }
      persistenceBuilder.addStore(builder);
   }

   private void parseBinaryKeyedJdbcStore(XMLExtendedStreamReader reader, PersistenceConfigurationBuilder persistenceBuilder)
         throws XMLStreamException {
      JdbcBinaryStoreConfigurationBuilder builder = new JdbcBinaryStoreConfigurationBuilder(
            persistenceBuilder);
      parseCommonJdbcStoreAttributes(reader, builder);
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case BINARY_KEYED_TABLE: {
            parseTable(reader, builder.table());
            break;
         }
         default: {
            parseCommonJdbcStoreElements(reader, element, builder);
            break;
         }
         }
      }
      persistenceBuilder.addStore(builder);
   }

   private void parseCommonJdbcStoreElements(XMLExtendedStreamReader reader, Element element, AbstractJdbcStoreConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      switch (element) {
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
         parseCommonStoreChildren(reader, builder);
         break;
      }
      }
   }

   private void parseCommonJdbcStoreAttributes(XMLExtendedStreamReader reader, AbstractJdbcStoreConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         parseCommonStoreAttributes(reader, i, builder);
      }
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

   private void parseMixedKeyedJdbcStore(XMLExtendedStreamReader reader, PersistenceConfigurationBuilder persistenceBuilder)
         throws XMLStreamException {
      JdbcMixedStoreConfigurationBuilder builder = new JdbcMixedStoreConfigurationBuilder(persistenceBuilder);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case KEY_TO_STRING_MAPPER:
            builder.key2StringMapper(value);
            break;
         default:
            parseCommonStoreAttributes(reader, i, builder);
            break;
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
         case STRING_KEYED_TABLE: {
            parseTable(reader, builder.stringTable());
            break;
         }
         case BINARY_KEYED_TABLE: {
            parseTable(reader, builder.binaryTable());
            break;
         }
         default: {
            parseCommonJdbcStoreElements(reader, element, builder);
            break;
         }
         }
      }
      persistenceBuilder.addStore(builder);
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

   /**
    * This method is public static so that it can be reused by custom cache store/loader configuration parsers
    */
   public static void parseCommonLoaderAttributes(XMLExtendedStreamReader reader, int i,
                                                  StoreConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      throw ParseUtils.unexpectedAttribute(reader, i);
   }

   /**
    * This method is public static so that it can be reused by custom cache store/loader configuration parsers
    */
   public static void parseCommonStoreAttributes(XMLExtendedStreamReader reader, int i,
                                                 StoreConfigurationBuilder<?, ?> builder) throws XMLStreamException {
      ParseUtils.requireNoNamespaceAttribute(reader, i);
      String value = replaceProperties(reader.getAttributeValue(i));
      org.infinispan.configuration.parsing.Attribute attribute = org.infinispan.configuration.parsing.Attribute.forName(reader.getAttributeLocalName(i));
      switch (attribute) {
         case FETCH_PERSISTENT_STATE:
            builder.fetchPersistentState(Boolean.parseBoolean(value));
            break;
         case IGNORE_MODIFICATIONS:
            builder.ignoreModifications(Boolean.parseBoolean(value));
            break;
         case PURGE_ON_STARTUP:
            builder.purgeOnStartup(Boolean.parseBoolean(value));
            break;
         case PRELOAD:
            builder.preload(Boolean.parseBoolean(value));
            break;
         case SHARED:
            builder.shared(Boolean.parseBoolean(value));
            break;
         default:
            throw ParseUtils.unexpectedAttribute(reader, i);
      }
   }


   public static void parseCommonStoreChildren(final XMLExtendedStreamReader reader,
                                               final StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      org.infinispan.configuration.parsing.Element element = org.infinispan.configuration.parsing.Element.forName(reader.getLocalName());
      switch (element) {
         case ASYNC:
            parseAsyncStore(reader, storeBuilder);
            break;
         case PROPERTIES:
            storeBuilder.withProperties(parseProperties(reader));
            break;
         case SINGLETON_STORE:
            parseSingletonStore(reader, storeBuilder);
            break;
         default:
            throw ParseUtils.unexpectedElement(reader);
      }
   }

   public static void parseAsyncStore(final XMLExtendedStreamReader reader, final StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         org.infinispan.configuration.parsing.Attribute attribute = org.infinispan.configuration.parsing.Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  storeBuilder.async().enable();
               } else {
                  storeBuilder.async().disable();
               }
               break;
            case FLUSH_LOCK_TIMEOUT:
               storeBuilder.async().flushLockTimeout(Long.parseLong(value));
               break;
            case MODIFICATION_QUEUE_SIZE:
               storeBuilder.async().modificationQueueSize(Integer.parseInt(value));
               break;
            case SHUTDOWN_TIMEOUT:
               storeBuilder.async().shutdownTimeout(Long.parseLong(value));
               break;
            case THREAD_POOL_SIZE:
               storeBuilder.async().threadPoolSize(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   public static void parseSingletonStore(final XMLExtendedStreamReader reader, final StoreConfigurationBuilder<?, ?> storeBuilder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         org.infinispan.configuration.parsing.Attribute attribute = org.infinispan.configuration.parsing.Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED:
               if (Boolean.parseBoolean(value)) {
                  storeBuilder.singleton().enable();
               } else {
                  storeBuilder.singleton().disable();
               }
               break;
            case PUSH_STATE_TIMEOUT:
               storeBuilder.singleton().pushStateTimeout(Long.parseLong(value));
               break;
            case PUSH_STATE_WHEN_COORDINATOR:
               storeBuilder.singleton().pushStateWhenCoordinator(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   public static Properties parseProperties(final XMLExtendedStreamReader reader) throws XMLStreamException {

      ParseUtils.requireNoAttributes(reader);

      Properties p = new Properties();
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         org.infinispan.configuration.parsing.Element element = org.infinispan.configuration.parsing.Element.forName(reader.getLocalName());
         switch (element) {
            case PROPERTY: {
               int attributes = reader.getAttributeCount();
               ParseUtils.requireAttributes(reader, org.infinispan.configuration.parsing.Attribute.NAME.getLocalName(), org.infinispan.configuration.parsing.Attribute.VALUE.getLocalName());
               String key = null;
               String propertyValue = null;
               for (int i = 0; i < attributes; i++) {
                  String value = replaceProperties(reader.getAttributeValue(i));
                  org.infinispan.configuration.parsing.Attribute attribute = org.infinispan.configuration.parsing.Attribute.forName(reader.getAttributeLocalName(i));
                  switch (attribute) {
                     case NAME: {
                        key = value;

                        break;
                     } case VALUE: {
                        propertyValue = value;
                        break;
                     }
                     default: {
                        throw ParseUtils.unexpectedAttribute(reader, i);
                     }
                  }
               }
               p.put(key, propertyValue);

               ParseUtils.requireNoContent(reader);

               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      return p;
   }
}
