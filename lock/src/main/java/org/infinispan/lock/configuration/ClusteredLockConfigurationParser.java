package org.infinispan.lock.configuration;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.lock.logging.Log;
import org.kohsuke.MetaInfServices;

/**
 * Clustered Locks configuration parser
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
@MetaInfServices
@Namespace(root = "clustered-locks")
@Namespace(uri = "urn:infinispan:config:clustered-locks:*", root = "clustered-locks", since = "9.4")
public class ClusteredLockConfigurationParser implements ConfigurationParser {

   private static final Log log = LogFactory.getLog(ClusteredLockConfigurationParser.class, Log.class);

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      if (holder.getScope() != ParserScope.CACHE_CONTAINER) {
         throw log.invalidScope(holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case CLUSTERED_LOCKS: {
            parseClusteredLocksElement(reader, builder.addModule(ClusteredLockManagerConfigurationBuilder.class));
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

   private void parseClusteredLocksElement(XMLStreamReader reader, ClusteredLockManagerConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NUM_OWNERS:
               builder.numOwner(Integer.parseInt(value));
               break;
            case RELIABILITY:
               builder.reliability(Reliability.valueOf(value));
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case CLUSTERED_LOCK:
               parseClusteredLock(reader, builder.addClusteredLock());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseClusteredLock(XMLStreamReader reader,
                                   ClusteredLockConfigurationBuilder builder) throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case NAME:
               builder.name(value);
               break;
            default:
               throw ParseUtils.unexpectedAttribute(reader, i);
         }
      }
      ParseUtils.requireNoContent(reader);
   }
}
