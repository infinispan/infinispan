package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser70;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Namespaces({
      @Namespace(uri = "urn:infinispan:config:soft-index:7.0",
                 root = SoftIndexFileStoreConfigurationParser70.ROOT_ELEMENT),
      @Namespace(root = SoftIndexFileStoreConfigurationParser70.ROOT_ELEMENT)
})
public class SoftIndexFileStoreConfigurationParser70 implements ConfigurationParser {
   public static final String ROOT_ELEMENT = "soft-index-file-store";

   public SoftIndexFileStoreConfigurationParser70() {
   }

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();
      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case SOFT_INDEX_FILE_STORE: {
         parseSoftIndexFileStore(reader, builder.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class));
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   private void parseSoftIndexFileStore(XMLExtendedStreamReader reader, SoftIndexFileStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         String attrName = reader.getAttributeLocalName(i);
         Attribute attribute = Attribute.forName(attrName);
         switch (attribute) {
            case OPEN_FILES_LIMIT:
               builder.openFilesLimit(Integer.parseInt(value));
               break;
            case COMPACTION_THRESHOLD:
               builder.compactionThreshold(Double.parseDouble(value));
               break;
            default:
               Parser70.parseStoreAttribute(reader, i, builder);
               break;
         }
      }

      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case DATA: {
               this.parseData(reader, builder);
               break;
            }
            case INDEX: {
               this.parseIndex(reader, builder);
               break;
            }
            default: {
               Parser70.parseStoreElement(reader, builder);
            }
         }
      }
   }

   private void parseData(XMLExtendedStreamReader reader, SoftIndexFileStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PATH:
               builder.dataLocation(value);
               break;
            case MAX_FILE_SIZE:
               builder.maxFileSize(Integer.parseInt(value));
               break;
            case SYNC_WRITES:
               builder.syncWrites(Boolean.parseBoolean(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseIndex(XMLExtendedStreamReader reader, SoftIndexFileStoreConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case PATH:
               builder.indexLocation(value);
               break;
            case SEGMENTS:
               builder.indexSegments(Integer.parseInt(value));
               break;
            case INDEX_QUEUE_LENGTH:
               builder.indexQueueLength(Integer.parseInt(value));
               break;
            case MIN_NODE_SIZE:
               builder.minNodeSize(Integer.parseInt(value));
               break;
            case MAX_NODE_SIZE:
               builder.maxNodeSize(Integer.parseInt(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }


}
