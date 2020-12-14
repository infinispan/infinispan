package org.infinispan.counter.configuration;

import static org.infinispan.counter.configuration.CounterConfigurationParser.NAMESPACE;
import static org.infinispan.counter.logging.Log.CONTAINER;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.parsing.Schema;
import org.infinispan.counter.api.Storage;
import org.kohsuke.MetaInfServices;

/**
 * Counters configuration parser
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@MetaInfServices
@Namespace(root = "counters")
@Namespace(uri = NAMESPACE + "*", root = "counters", since = "9.0")
public class CounterConfigurationParser implements ConfigurationParser {

   static final String NAMESPACE = Parser.NAMESPACE + "counters:";

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      if (!holder.inScope(ParserScope.CACHE_CONTAINER)) {
         throw CONTAINER.invalidScope(holder.getScope());
      }
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();

      Element element = Element.forName(reader.getLocalName());
      switch (element) {
         case COUNTERS: {
            parseCountersElement(reader, builder.addModule(CounterManagerConfigurationBuilder.class));
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

   /**
    * Reads a list of counter's configuration from an {@link InputStream}.
    *
    * @param is the {@link InputStream} to read.
    * @return a {@link List} of {@link AbstractCounterConfiguration} read.
    */
   public List<AbstractCounterConfiguration> parseConfigurations(InputStream is) throws IOException {
      BufferedInputStream input = new BufferedInputStream(is);
      ConfigurationReader reader = ConfigurationReader.from(input).build();
      CounterManagerConfigurationBuilder builder = new CounterManagerConfigurationBuilder(null);
      try {
         reader.require(ConfigurationReader.ElementType.START_DOCUMENT);
         reader.nextElement();
         reader.require(ConfigurationReader.ElementType.START_ELEMENT);
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case COUNTERS: {
               parseCountersElement(reader, builder);
               break;
            }
            default: {
               throw ParseUtils.unexpectedElement(reader);
            }
         }
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
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case STRONG_COUNTER:
               Schema schema = getSchema(reader);
               if (!schema.since(10, 0)) {
                  parseStrongCounterLegacy(reader, builder.addStrongCounter());
               } else {
                  parseStrongCounter(reader, builder.addStrongCounter());
               }
               break;
            case WEAK_COUNTER:
               parseWeakCounter(reader, builder.addWeakCounter());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private Schema getSchema(ConfigurationReader reader) {
      String namespaceURI = reader.getNamespace();
      if (namespaceURI == null)
         return new Schema(NAMESPACE, Integer.parseInt(Version.getMajor()), Integer.parseInt(Version.getMinor()));
      return Schema.fromNamespaceURI(namespaceURI);
   }

   private void parseWeakCounter(ConfigurationReader reader, WeakCounterConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case CONCURRENCY_LEVEL:
               builder.concurrencyLevel(Integer.parseInt(value));
               break;
            default:
               parserCommonCounterAttributes(reader, builder, i, attribute, value);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseStrongCounterLegacy(ConfigurationReader reader, StrongCounterConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         parserCommonCounterAttributes(reader, builder, i, attribute, value);
      }
      while (reader.inTag()) {
         Element element = Element.forName(reader.getLocalName());
         switch (element) {
            case UPPER_BOUND:
               parseUpperBound(reader, builder);
               break;
            case LOWER_BOUND:
               parseLowerBound(reader, builder);
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseUpperBound(ConfigurationReader reader, StrongCounterConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case VALUE:
               builder.upperBound(Long.parseLong(value));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parseLowerBound(ConfigurationReader reader, StrongCounterConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case VALUE:
               builder.lowerBound(Long.parseLong(value));
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
      ParseUtils.requireNoContent(reader);
   }


   private void parseStrongCounter(ConfigurationReader reader, StrongCounterConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         switch (attribute) {
            case UPPER_BOUND:
               builder.upperBound(Long.parseLong(value));
               break;
            case LOWER_BOUND:
               builder.lowerBound(Long.parseLong(value));
               break;
            default:
               parserCommonCounterAttributes(reader, builder, i, attribute, value);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   private void parserCommonCounterAttributes(ConfigurationReader reader, CounterConfigurationBuilder builder,
                                              int index, Attribute attribute, String value) {
      switch (attribute) {
         case NAME:
            builder.name(value);
            break;
         case INITIAL_VALUE:
            builder.initialValue(Long.parseLong(value));
            break;
         case STORAGE:
            builder.storage(Storage.valueOf(value));
            break;
         default:
            throw ParseUtils.unexpectedAttribute(reader, index);
      }
   }
}
