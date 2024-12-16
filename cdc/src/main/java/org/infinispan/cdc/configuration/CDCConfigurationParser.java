package org.infinispan.cdc.configuration;

import static org.infinispan.cdc.configuration.CDCConfigurationParser.NAMESPACE;

import java.util.Properties;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.CacheParser;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationParser;
import org.kohsuke.MetaInfServices;

@MetaInfServices(ConfigurationParser.class)
@Namespace(root = "change-data-capture", since = "16.0")
@Namespace(uri = NAMESPACE + "*", root = "change-data-capture", since = "16.0")
public class CDCConfigurationParser extends AbstractJdbcStoreConfigurationParser {

   static final String PREFIX = "cdc";
   static final String NAMESPACE = Parser.NAMESPACE + PREFIX + ":";

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (!holder.inScope(ParserScope.CACHE) && !holder.inScope(ParserScope.CACHE_TEMPLATE))
         throw new IllegalStateException("Scoped invalid to enable change data capture");

      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case CDC -> {
            ChangeDataCaptureConfigurationBuilder cdcBuilder = builder.addModule(ChangeDataCaptureConfigurationBuilder.class);
            cdcBuilder.enabled(true);
            parseCdcElement(reader, cdcBuilder);
         }
         default -> throw ParseUtils.unexpectedElement(reader);
      }
   }

   private void parseCdcElement(ConfigurationReader reader, ChangeDataCaptureConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case ENABLED -> builder.enabled(Boolean.parseBoolean(value));
            case FOREIGN_KEYS -> builder.addForeignKey(value);
            default -> CacheParser.parseStoreAttribute(reader, i, builder);
         }
      }

      while (reader.inTag()) {
         String name = reader.getLocalName();
         Element element = Element.forName(name);
         switch (element) {
            case UNKNOWN -> {
               var el = org.infinispan.persistence.jdbc.common.configuration.Element.forName(name);
               if (!handleCommonElement(builder, el, reader))
                  throw ParseUtils.unexpectedElement(reader, name);
            }
            case TABLE -> parseTableElement(reader, builder.table());
            case CONNECTOR_PROPERTIES -> parseConnectorPropertiesElement(reader, builder.connectorProperties());
            default -> throw ParseUtils.unexpectedElement(reader, name);
         }
      }
   }

   private void parseTableElement(ConfigurationReader reader, TableConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME -> builder.name(value);
            default -> throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      while (reader.inTag()) {
         String name = reader.getLocalName();
         Element element = Element.forName(name);
         switch (element) {
            case PRIMARY_KEY -> parseColumnElement(reader, builder.primaryKey());
            case COLUMN -> parseColumnElement(reader, builder.addColumn());
            default -> throw ParseUtils.unexpectedElement(reader, name);
         }
      }
   }

   private void parseColumnElement(ConfigurationReader reader, ColumnConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case NAME -> builder.name(value);
            default -> throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }

      ParseUtils.requireNoContent(reader);
   }

   private void parseConnectorPropertiesElement(ConfigurationReader reader, Properties properties) {
      while (reader.inTag()) {
         String name = reader.getLocalName();
         String value = reader.getElementText();
         properties.setProperty(name, value);
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
