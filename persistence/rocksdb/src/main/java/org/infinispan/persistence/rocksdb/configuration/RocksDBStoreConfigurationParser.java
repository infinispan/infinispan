package org.infinispan.persistence.rocksdb.configuration;

import static org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationParser.NAMESPACE;
import static org.infinispan.util.logging.Log.CONFIG;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.kohsuke.MetaInfServices;

/**
 * RocksDB XML Parser
 * @author Tristan Tarrant
 * @since 9.0
 */
@MetaInfServices
@Namespace(root = "rocksdb-store")
@Namespace(uri = NAMESPACE + "*", root = "rocksdb-store", since = "9.0")
public class RocksDBStoreConfigurationParser implements ConfigurationParser {

   static final String NAMESPACE = Parser.NAMESPACE + "store:rocksdb:";

   public RocksDBStoreConfigurationParser() {
   }

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case ROCKSDB_STORE: {
            parseRocksDBCacheStore(reader, builder.persistence().addStore(RocksDBStoreConfigurationBuilder.class));
            break;
         }
         default: {
            throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseRocksDBCacheStore(XMLExtendedStreamReader reader, RocksDBStoreConfigurationBuilder builder) throws XMLStreamException {
      String path = null;
      String relativeTo = null;
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         String attrName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attrName);

         switch (attribute) {
            case PATH: {
               path = value;
               break;
            }
            case RELATIVE_TO: {
               relativeTo = ParseUtils.requireAttributeProperty(reader, i);
               break;
            }
            case CLEAR_THRESHOLD: {
               builder.clearThreshold(Integer.parseInt(value));
               break;
            }
            case BLOCK_SIZE: {
               builder.blockSize(Integer.parseInt(value));
               break;
            }
            case CACHE_SIZE: {
               builder.cacheSize(Long.parseLong(value));
               break;
            }
            default: {
               Parser.parseStoreAttribute(reader, i, builder);
            }
         }
      }
      path = ParseUtils.resolvePath(path, relativeTo);
      if (path != null) {
         builder.location(path);
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case EXPIRATION: {
               this.parseExpiry(reader, builder);
               break;
            }
            case COMPRESSION: {
               this.parseCompression(reader, builder);
               break;
            }
            default: {
               Parser.parseStoreElement(reader, builder);
            }
         }
      }
   }

   private void parseExpiry(XMLExtendedStreamReader reader, RocksDBStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PATH: {
               builder.expiredLocation(value);
               break;
            }
            case QUEUE_SIZE: {
               CONFIG.ignoreXmlAttribute(attribute, reader.getLocation().getLineNumber(), reader.getLocation().getColumnNumber());
               break;
            }
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseCompression(XMLExtendedStreamReader reader, RocksDBStoreConfigurationBuilder builder) throws XMLStreamException {
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

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   @Override
   public Class<? extends ConfigurationBuilderInfo> getConfigurationBuilderInfo() {
      return RocksDBStoreConfigurationBuilder.class;
   }
}
