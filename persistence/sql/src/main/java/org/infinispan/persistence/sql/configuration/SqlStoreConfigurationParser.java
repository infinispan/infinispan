package org.infinispan.persistence.sql.configuration;

import static org.infinispan.persistence.sql.configuration.SqlStoreConfigurationParser.NAMESPACE;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.CacheParser;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationParser;
import org.infinispan.persistence.jdbc.common.configuration.Attribute;
import org.infinispan.persistence.jdbc.common.configuration.Element;
import org.kohsuke.MetaInfServices;

@MetaInfServices(ConfigurationParser.class)
@Namespace(root = "table-jdbc-store")
@Namespace(root = "query-jdbc-store")
@Namespace(uri = NAMESPACE + "*", root = "table-jdbc-store")
@Namespace(uri = NAMESPACE + "*", root = "query-jdbc-store")
public class SqlStoreConfigurationParser extends AbstractJdbcStoreConfigurationParser {

   static final String NAMESPACE = Parser.NAMESPACE + "store:sql:";

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case TABLE_JDBC_STORE: {
            parseTableJdbcStore(reader, builder.persistence());
            break;
         }
         case QUERY_JDBC_STORE: {
            parseQueryJdbcStore(reader, builder.persistence());
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseTableJdbcStore(ConfigurationReader reader, PersistenceConfigurationBuilder persistenceBuilder) {
      TableJdbcStoreConfigurationBuilder builder = persistenceBuilder.addStore(TableJdbcStoreConfigurationBuilder.class);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         if (!handleCommonAttributes(reader, builder, attribute, value)) {
            if (attribute == Attribute.TABLE_NAME) {
               builder.tableName(value);
            } else {
               CacheParser.parseStoreAttribute(reader, i, builder);
            }
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         if (!handleCommonElement(builder, element, reader)) {
            if (element == Element.SCHEMA) {
               parseSchema(reader, builder.schema());
            } else {
               CacheParser.parseStoreElement(reader, builder);
            }
         }
      }
   }

   private void parseQueryJdbcStore(ConfigurationReader reader, PersistenceConfigurationBuilder persistenceBuilder) {
      QueriesJdbcStoreConfigurationBuilder builder = persistenceBuilder.addStore(QueriesJdbcStoreConfigurationBuilder.class);
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         if (!handleCommonAttributes(reader, builder, attribute, value)) {
            if (attribute == Attribute.KEY_COLUMNS) {
               builder.keyColumns(value);
            } else {
               CacheParser.parseStoreAttribute(reader, i, builder);
            }
         }
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         if (!handleCommonElement(builder, element, reader)) {
            switch (element) {
               case QUERIES:
                  parseQueries(reader, builder.queries());
                  break;
               case SCHEMA:
                  parseSchema(reader, builder.schema());
                  break;
               default:
                  CacheParser.parseStoreElement(reader, builder);
                  break;
            }
         }
      }
   }

   private void parseQueries(ConfigurationReader reader, QueriesJdbcConfigurationBuilder builder) {
      // YAML and JSON this has to be an attribute so support both named attributes under queries
      // as well as elements with XML
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case SELECT_ALL:
               builder.selectAll(value);
               break;
            case SELECT_SINGLE:
               builder.select(value);
               break;
            case DELETE_ALL:
               builder.deleteAll(value);
               break;
            case DELETE_SINGLE:
               builder.delete(value);
               break;
            case UPSERT:
               builder.upsert(value);
               break;
            case SIZE:
               builder.size(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      if (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   private void parseSchema(ConfigurationReader reader, SchemaJdbcConfigurationBuilder<?> builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case KEY_MESSAGE_NAME:
               builder.keyMessageName(value);
               break;
            case MESSAGE_NAME:
               builder.messageName(value);
               break;
            case EMBEDDED_KEY:
               builder.embeddedKey(Boolean.parseBoolean(value));
               break;
            case PACKAGE:
               builder.packageName(value);
               break;
         }
      }
      if (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         throw ParseUtils.unexpectedElement(reader, element);
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
