package org.infinispan.loaders.mongodb.configuration;

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
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

/**
 * Parses the configuration from the XML. For valid elements and attributes refer to {@link Element} and {@link Attribute}
 * @since 5.3
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:mongodb:5.3", root = "mongodbStore"),
})
public class MongoDBCacheStoreConfigurationParser53 implements ConfigurationParser {

   @Override
   public void readElement(XMLExtendedStreamReader xmlExtendedStreamReader, ConfigurationBuilderHolder configurationBuilderHolder)
         throws XMLStreamException {
      ConfigurationBuilder builder = configurationBuilderHolder.getCurrentConfigurationBuilder();

      Element element = Element.forName(xmlExtendedStreamReader.getLocalName());
      switch (element) {
         case MONGODB_STORE: {
            parseMongoDBStore(
                  xmlExtendedStreamReader,
                  builder.loaders(),
                  configurationBuilderHolder.getClassLoader()
            );
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(xmlExtendedStreamReader);
         }
      }
   }

   private void parseMongoDBStore(XMLExtendedStreamReader reader, LoadersConfigurationBuilder loadersBuilder, ClassLoader classLoader)
         throws XMLStreamException {
      MongoDBCacheStoreConfigurationBuilder builder = new MongoDBCacheStoreConfigurationBuilder(loadersBuilder);

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CONNECTION: {
               this.parseConnection(reader, builder);
               break;
            }
            case AUTHENTICATION: {
               this.parseAuthentication(reader, builder);
               break;
            }
            case STORAGE: {
               this.parseStorage(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      loadersBuilder.addStore(builder);
   }

   private void parseStorage(XMLExtendedStreamReader reader, MongoDBCacheStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case DATABASE: {
               builder.database(value);
               break;
            }
            case COLLECTION: {
               builder.collection(value);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseConnection(XMLExtendedStreamReader reader, MongoDBCacheStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case HOST: {
               builder.host(value);
               break;
            }
            case PORT: {
               builder.port(Integer.valueOf(value));
               break;
            }
            case TIMEOUT: {
               builder.timeout(Integer.valueOf(value));
               break;
            }
            case ACKNOWLEDGMENT: {
               builder.acknowledgment(Integer.valueOf(value));
               break;
            }
            default: {
               throw ParseUtils.unexpectedAttribute(reader, i);
            }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseAuthentication(XMLExtendedStreamReader reader, MongoDBCacheStoreConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case USERNAME: {
               builder.username(value);
               break;
            }
            case PASSWORD: {
               builder.password(value);
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
