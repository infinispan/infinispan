package org.infinispan.tools.config.v6.leveldb;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.persistence.leveldb.configuration.CompressionType;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.tools.config.v6.Parser60;
import org.kohsuke.MetaInfServices;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@MetaInfServices
@Namespace(uri = "urn:infinispan:config:store:leveldb:6.0", root = "leveldbStore")
public class LevelDBStoreConfigurationParser60 implements ConfigurationParser {

   public LevelDBStoreConfigurationParser60() {
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
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
         String value = reader.getAttributeValue(i);
         String attrName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attrName);

         switch (attribute) {
            case LOCATION: {
               builder.location(value);
               break;
            }
            case EXPIRED_LOCATION: {
               builder.expiredLocation(value);
               break;
            }
            case IMPLEMENTATION_TYPE: {
               builder.implementationType(LevelDBStoreConfiguration.ImplementationType.valueOf(value));
               break;
            }
            case CLEAR_THRESHOLD: {
               builder.clearThreshold(Integer.valueOf(value));
               break;
            }
            case EXPIRY_QUEUE_SIZE: {
               builder.expiryQueueSize(Integer.valueOf(value));
            }
            case BLOCK_SIZE: {
               builder.blockSize(Integer.valueOf(value));
               break;
            }
            case CACHE_SIZE: {
               builder.cacheSize(Long.valueOf(value));
               break;
            }
            case COMPRESSION_TYPE: {
               builder.compressionType(CompressionType.valueOf(value));
               break;
            }
            default: {
               Parser60.parseCommonStoreAttributes(reader, builder, attrName, value, i);
            }
         }
      }

      if (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         ParseUtils.unexpectedElement(reader);
      }
   }
}
