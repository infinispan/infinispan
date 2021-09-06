package org.infinispan.counter.configuration;

import static org.infinispan.counter.configuration.CounterConfigurationParser.NAMESPACE;
import static org.infinispan.counter.logging.Log.CONTAINER;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserScope;
import org.kohsuke.MetaInfServices;

/**
 * Counters configuration parser
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
@MetaInfServices(ConfigurationParser.class)
@Namespace(root = "counters")
@Namespace(uri = NAMESPACE + "*", root = "counters", since = "9.0")
public class CounterConfigurationParser extends CounterParser {

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (!holder.inScope(ParserScope.CACHE_CONTAINER)) {
         throw CONTAINER.invalidScope(holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      if (element != Element.COUNTERS) {
         throw ParseUtils.unexpectedElement(reader);
      }
      parseCountersElement(reader, builder.addModule(CounterManagerConfigurationBuilder.class));
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   /**
    * Reads a list of counter's configuration from an {@link InputStream}.
    *
    * @param is the {@link InputStream} to read.
    * @return a {@link List} of {@link AbstractCounterConfiguration} read.
    */
   public Map<String, AbstractCounterConfiguration> parseConfigurations(InputStream is) throws IOException {
      BufferedInputStream input = new BufferedInputStream(is);
      ConfigurationReader reader = ConfigurationReader.from(input).build();
      CounterManagerConfigurationBuilder builder = new CounterManagerConfigurationBuilder(null);
      try {
         reader.require(ConfigurationReader.ElementType.START_DOCUMENT);
         reader.nextElement();
         reader.require(ConfigurationReader.ElementType.START_ELEMENT);
         Element element = Element.forName(reader.getLocalName());
         if (element != Element.COUNTERS) {
            throw ParseUtils.unexpectedElement(reader);
         }
         parseCountersElement(reader, builder);
      } finally {
         Util.close(reader);
      }
      return builder.create().counters();
   }

   private void parseCountersElement(ConfigurationReader reader, CounterManagerConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
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
      if (reader.hasFeature(ConfigurationFormatFeature.MIXED_ELEMENTS)) {
         while (reader.inTag()) {
            Map.Entry<String, String> item = reader.getMapItem(Attribute.NAME);
            readElement(reader, builder, Element.forName(item.getValue()), item.getKey());
            reader.endMapItem();
         }
      } else {
         reader.nextElement();
         reader.require(ConfigurationReader.ElementType.START_ELEMENT, null, Element.COUNTERS);
         while (reader.inTag()) {
            Map.Entry<String, String> item = reader.getMapItem(Attribute.NAME);
            readElement(reader, builder, Element.forName(item.getValue()), item.getKey());
            reader.endMapItem();
         }
         reader.nextElement();
         reader.require(ConfigurationReader.ElementType.END_ELEMENT, null, Element.COUNTERS);
      }
   }
}
