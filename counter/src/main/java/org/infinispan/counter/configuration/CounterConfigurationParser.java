package org.infinispan.counter.configuration;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.Namespaces;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.parsing.XMLExtendedStreamReader;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.logging.Log;
import org.kohsuke.MetaInfServices;

/**
 * Counters configuration parser
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@MetaInfServices
@Namespaces({
      @Namespace(root = "counters"),
      @Namespace(uri = "urn:infinispan:config:counters:9.2", root = "counters"),
      @Namespace(uri = "urn:infinispan:config:counters:9.1", root = "counters"),
      @Namespace(uri = "urn:infinispan:config:counters:9.0", root = "counters")
})
public class CounterConfigurationParser implements ConfigurationParser {

   private static final Log log = LogFactory.getLog(CounterConfigurationParser.class, Log.class);

   @Override
   public void readElement(XMLExtendedStreamReader reader, ConfigurationBuilderHolder holder)
         throws XMLStreamException {
      if (holder.getScope() != ParserScope.CACHE_CONTAINER) {
         throw log.invalidScope(holder.getScope());
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

   private void parseCountersElement(XMLExtendedStreamReader reader, CounterManagerConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
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
            case STRONG_COUNTER:
               parseStrongCounter(reader, builder.addStrongCounter());
               break;
            case WEAK_COUNTER:
               parseWeakCounter(reader, builder.addWeakCounter());
               break;
            default:
               throw ParseUtils.unexpectedElement(reader);
         }
      }
   }

   private void parseWeakCounter(XMLExtendedStreamReader reader, WeakCounterConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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

   private void parseStrongCounter(XMLExtendedStreamReader reader, StrongCounterConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
         parserCommonCounterAttributes(reader, builder, i, attribute, value);
      }
      while (reader.hasNext() && (reader.nextTag() != XMLStreamConstants.END_ELEMENT)) {
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

   private void parseUpperBound(XMLExtendedStreamReader reader, StrongCounterConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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

   private void parseLowerBound(XMLExtendedStreamReader reader, StrongCounterConfigurationBuilder builder)
         throws XMLStreamException {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = replaceProperties(reader.getAttributeValue(i));
         Attribute attribute = Attribute.forName(reader.getAttributeLocalName(i));
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

   private void parserCommonCounterAttributes(XMLExtendedStreamReader reader, CounterConfigurationBuilder builder,
         int index, Attribute attribute, String value)
         throws XMLStreamException {
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
