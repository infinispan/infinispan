package org.infinispan.anchored.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.kohsuke.MetaInfServices;

/**
 * Anchored keys parser extension.
 *
 * This extension parses elements in the "urn:infinispan:config:anchored" namespace
 *
 * @author Dan Berindei
 * @since 11
 */
@MetaInfServices
@Namespace(root = "anchored-keys")
@Namespace(uri = "urn:infinispan:config:anchored:*", root = "anchored-keys", since = "11.0")
public class AnchoredKeysConfigurationParser implements ConfigurationParser {

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder) throws XMLStreamException {
      if (!holder.inScope(ParserScope.CACHE) && !holder.inScope(ParserScope.CACHE_TEMPLATE)) {
         throw new IllegalStateException("WRONG SCOPE");
      }
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case ANCHORED_KEYS: {
         parseAnchoredKeys(reader, builder.addModule(AnchoredKeysConfigurationBuilder.class));
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseAnchoredKeys(XMLExtendedStreamReader reader, AnchoredKeysConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         switch (attribute) {
            case ENABLED: {
            builder.enabled(Boolean.parseBoolean(value));
            break;
         }
         default: {
            throw ParseUtils.unexpectedAttribute(reader, i);
         }
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }
}
