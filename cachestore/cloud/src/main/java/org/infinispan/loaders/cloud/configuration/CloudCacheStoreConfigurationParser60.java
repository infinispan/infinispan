package org.infinispan.loaders.cloud.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser52;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

/**
 *
 * CloudCacheStoreConfigurationParser60.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:cloud:6.0", root = "cloudStore"),
   @Namespace(root = "cloudStore"),
})
public class CloudCacheStoreConfigurationParser60 implements ConfigurationParser {

   public CloudCacheStoreConfigurationParser60() {
   }

   @Override
   public void readElement(final XMLExtendedStreamReader reader, final ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case CLOUD_STORE: {
         parseCloudStore(reader, builder.loaders(), holder.getClassLoader());
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseCloudStore(final XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder,
         ClassLoader classLoader) throws XMLStreamException {
      CloudCacheStoreConfigurationBuilder builder = new CloudCacheStoreConfigurationBuilder(loadersBuilder);
      parseCloudStoreAttributes(reader, builder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Parser52.parseCommonStoreChildren(reader, builder);
      }
      loadersBuilder.addStore(builder);
   }

   private void parseCloudStoreAttributes(XMLExtendedStreamReader reader, CloudCacheStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
         case BUCKET_PREFIX: {
            builder.bucketPrefix(value);
            break;
         }
         case CLOUD_SERVICE: {
            builder.cloudService(value);
            break;
         }
         case CLOUD_SERVICE_LOCATION: {
            builder.cloudServiceLocation(value);
            break;
         }
         case COMPRESS: {
            builder.compress(Boolean.parseBoolean(value));
            break;
         }
         case IDENTITY: {
            builder.identity(value);
            break;
         }
         case LAZY_PURGING_ONLY: {
            builder.lazyPurgingOnly(Boolean.parseBoolean(value));
            break;
         }
         case MAX_CONNECTIONS: {
            builder.maxConnections(Integer.parseInt(value));
            break;
         }
         case PASSWORD: {
            builder.password(value);
            break;
         }
         case PROXY_HOST: {
            builder.proxyHost(value);
            break;
         }
         case PROXY_PORT: {
            builder.proxyPort(Integer.parseInt(value));
            break;
         }
         case REQUEST_TIMEOUT: {
            builder.requestTimeout(Long.parseLong(value));
            break;
         }
         case SECURE: {
            builder.secure(Boolean.parseBoolean(value));
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
