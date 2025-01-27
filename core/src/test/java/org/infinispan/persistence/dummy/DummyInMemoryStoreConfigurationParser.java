package org.infinispan.persistence.dummy;

import static org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationParser.NAMESPACE;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.CacheParser;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.kohsuke.MetaInfServices;

/**
 * @author William Burns
 * @since 14.0
 */
@MetaInfServices(ConfigurationParser.class)
@Namespace(root = "dummy-store")
@Namespace(uri = NAMESPACE + "*", root = "dummy-store")
public class DummyInMemoryStoreConfigurationParser implements ConfigurationParser {

   static final String NAMESPACE = Parser.NAMESPACE + "store:dummy:";

   @Override
   public void readElement(ConfigurationReader reader,
         ConfigurationBuilderHolder holder) {
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case DUMMY_STORE: {
            ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
            parseDummyCacheStore(reader, builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class));
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseDummyCacheStore(ConfigurationReader reader, DummyInMemoryStoreConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));

         switch (attribute) {
            case SLOW:
               builder.slow(Boolean.parseBoolean(value));
               break;
            case START_FAILURES:
               builder.startFailures(Integer.parseInt(value));
               break;
            case STORE_NAME:
               builder.storeName(value);
               break;
            default: {
               CacheParser.parseStoreAttribute(reader, i, builder);
            }
         }
      }

      while (reader.inTag()) {
         CacheParser.parseStoreElement(reader, builder);
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
