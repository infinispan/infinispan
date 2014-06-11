package org.infinispan.persistence.leveldb.configuration;

import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser70;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Namespaces({
      @Namespace(uri = "urn:infinispan:config:store:leveldb:7.0", root = "leveldb-store"),
      @Namespace(root = "leveldb-store")
})
public class LevelDBStoreConfigurationParser70 implements ConfigurationParser {

   private static final Log log = LogFactory.getLog(LevelDBStoreConfigurationParser70.class);

   public LevelDBStoreConfigurationParser70() {
   }

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case LEVELDB_STORE: {
            parseLevelDBCacheStore(reader, builder.persistence().addStore(LevelDBStoreConfigurationBuilder.class));
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseLevelDBCacheStore(XMLExtendedStreamReader reader, LevelDBStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String attributeValue = reader.getAttributeValue(i);
         String value = StringPropertyReplacer.replaceProperties(attributeValue);
         String attrName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attrName);

         switch (attribute) {
            case PATH: {
               builder.location(value);
               break;
            }
            case RELATIVE_TO: {
               log.ignoreXmlAttribute(attribute);
               break;
            }
            case CLEAR_THRESHOLD: {
               builder.clearThreshold(Integer.valueOf(value));
               break;
            }
            case BLOCK_SIZE: {
               builder.blockSize(Integer.valueOf(value));
               break;
            }
            case CACHE_SIZE: {
               builder.cacheSize(Long.valueOf(value));
               break;
            }
            default: {
               Parser70.parseStoreAttribute(reader, i, builder);
            }
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case EXPIRATION: {
               this.parseLevelDBCacheStoreExpiry(reader, builder);
               break;
            }
            case COMPRESSION: {
               this.parseLevelDBCacheStoreCompression(reader, builder);
               break;
            }
            case IMPLEMENTATION: {
               this.parseLevelDBCacheStoreImplementation(reader, builder);
               break;
            }
            default: {
               Parser70.parseStoreElement(reader, builder);
            }
         }
      }
   }

   private void parseLevelDBCacheStoreExpiry(XMLExtendedStreamReader reader, LevelDBStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PATH: {
               builder.expiredLocation(value);
               break;
            }
            case QUEUE_SIZE: {
               builder.expiryQueueSize(Integer.valueOf(value));
               break;
            }
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseLevelDBCacheStoreCompression(XMLExtendedStreamReader reader, LevelDBStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case TYPE: {
               builder.compressionType(CompressionType.valueOf(value));
               break;
            }
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseLevelDBCacheStoreImplementation(XMLExtendedStreamReader reader, LevelDBStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case TYPE: {
               builder.implementationType(LevelDBStoreConfiguration.ImplementationType.valueOf(value));
               break;
            }
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

}
