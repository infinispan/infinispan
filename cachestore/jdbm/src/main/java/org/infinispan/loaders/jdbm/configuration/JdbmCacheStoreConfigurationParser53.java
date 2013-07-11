package org.infinispan.loaders.jdbm.configuration;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser52;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

/**
 *
 * JdbmCacheStoreConfigurationParser53.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:jdbm:5.3", root = "jdbmStore"),
})
public class JdbmCacheStoreConfigurationParser53 implements ConfigurationParser {

   public JdbmCacheStoreConfigurationParser53() {
   }


   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case JDBM_STORE: {
         parseJdbmStore(reader, builder.loaders(), holder.getClassLoader());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseJdbmStore(final XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder,
         ClassLoader classLoader) throws XMLStreamException {
      JdbmCacheStoreConfigurationBuilder builder = new JdbmCacheStoreConfigurationBuilder(loadersBuilder);
      parseBdbjeStoreAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Parser52.parseCommonStoreChildren(reader, builder);
      }
      loadersBuilder.addStore(builder);
   }

   private void parseBdbjeStoreAttributes(XMLExtendedStreamReader reader, JdbmCacheStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case COMPARATOR_CLASS_NAME: {
            builder.comparatorClassName(value);
            break;
         }
         case EXPIRY_QUEUE_SIZE: {
            builder.expiryQueueSize(Integer.parseInt(value));
            break;
         }
         case LOCATION: {
            builder.location(value);
            break;
         }
         default: {
            Parser52.parseCommonStoreAttributes(reader, i, builder);
            break;
         }
         }
      }
   }
}
