package org.infinispan.counter.configuration;

import static javax.xml.stream.XMLStreamConstants.START_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;
import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.List;

import javax.xml.stream.XMLInputFactory;
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
@Namespace(root = "counters")
@Namespace(uri = "urn:infinispan:config:counters:*", root = "counters", since = "9.0")
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

   /**
    * Reads a list of counter's configuration from an {@link InputStream}.
    *
    * @param is the {@link InputStream} to read.
    * @return a {@link List} of {@link AbstractCounterConfiguration} read.
    * @throws XMLStreamException if the xml is malformed.
    */
   public List<AbstractCounterConfiguration> parseConfigurations(InputStream is) throws XMLStreamException {
      BufferedInputStream input = new BufferedInputStream(is);
      XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(input);
      CounterManagerConfigurationBuilder builder = new CounterManagerConfigurationBuilder(null);
      try {
         reader.require(START_DOCUMENT, null, null);
         reader.nextTag();
         reader.require(START_ELEMENT, null, null);
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
         reader.close();
      }
      return builder.create().counters();
   }

   private void parseCountersElement(XMLStreamReader reader, CounterManagerConfigurationBuilder builder)
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

   private void parseWeakCounter(XMLStreamReader reader, WeakCounterConfigurationBuilder builder)
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

   private void parseStrongCounter(XMLStreamReader reader, StrongCounterConfigurationBuilder builder)
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

   private void parseUpperBound(XMLStreamReader reader, StrongCounterConfigurationBuilder builder)
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

   private void parseLowerBound(XMLStreamReader reader, StrongCounterConfigurationBuilder builder)
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

   private void parserCommonCounterAttributes(XMLStreamReader reader, CounterConfigurationBuilder builder,
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
