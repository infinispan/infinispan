package org.infinispan.loaders.leveldb.configuration;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser53;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.loaders.leveldb.LevelDBCacheStoreConfig.ImplementationType;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.iq80.leveldb.CompressionType;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Namespaces({
   @Namespace(uri = "urn:infinispan:config:leveldb:5.3", root = "leveldbStore"),
   @Namespace(root = "leveldbStore")
})
public class LevelDBCacheStoreConfigurationParser53 implements ConfigurationParser {

	public LevelDBCacheStoreConfigurationParser53() {
	}

	@Override
	public void readElement(XMLExtendedStreamReader reader,
			ConfigurationBuilderHolder holder) throws XMLStreamException {
		ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
		Element element = Element.forName(reader.getLocalName());
		switch (element) {
		case LEVELDB_STORE: {
			parseLevelDBCacheStore(
					reader,
					builder.loaders().addLoader(
							LevelDBCacheStoreConfigurationBuilder.class));
			break;
		}
		default: {
			throw ParseUtils.unexpectedElement(reader);
		}
		}
	}

	private void parseLevelDBCacheStore(XMLExtendedStreamReader reader,
			LevelDBCacheStoreConfigurationBuilder builder)
			throws XMLStreamException {
		for (int i = 0; i < reader.getAttributeCount(); i++) {
			ParseUtils.requireNoNamespaceAttribute(reader, i);
			String value = StringPropertyReplacer.replaceProperties(reader
					.getAttributeValue(i));
			Attribute attribute = Attribute.forName(reader
					.getAttributeLocalName(i));

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
               builder.implementationType(ImplementationType.valueOf(value));
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
               Parser53.parseCommonStoreAttributes(reader, i, builder);
            }
         }
      }

		if (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         ParseUtils.unexpectedElement(reader);
      }
	}
}
