package org.infinispan.anchored.configuration;


import static org.infinispan.anchored.configuration.AnchoredKeysConfigurationParser.NAMESPACE;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.ParserScope;
import org.kohsuke.MetaInfServices;

/**
 * Anchored keys parser extension.
 *
 * This extension parses elements in the "urn:infinispan:config:anchored-keys" namespace
 *
 * @author Dan Berindei
 * @since 11
 */
@MetaInfServices
@Namespace(root = "anchored-keys")
@Namespace(uri = NAMESPACE + "*", root = "anchored-keys", since = "11.0")
public class AnchoredKeysConfigurationParser implements ConfigurationParser {

   static final String PREFIX = "anchored-keys";
   static final String NAMESPACE = Parser.NAMESPACE + PREFIX + ":";

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (!holder.inScope(ParserScope.CACHE) && !holder.inScope(ParserScope.CACHE_TEMPLATE)) {
         throw new IllegalStateException("WRONG SCOPE");
      }
      ConfigurationBuilder builder = holder.getCurrentConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
      case ANCHORED_KEYS: {
         AnchoredKeysConfigurationBuilder anchoredBuilder = builder.addModule(AnchoredKeysConfigurationBuilder.class);
         // Do not require an explicit enabled="true" attribute
         anchoredBuilder.enabled(true);
         parseAnchoredKeys(reader, anchoredBuilder);
         break;
      }
      default: {
         throw ParseUtils.unexpectedElement(reader);
      }
      }
   }

   private void parseAnchoredKeys(ConfigurationReader reader, AnchoredKeysConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
