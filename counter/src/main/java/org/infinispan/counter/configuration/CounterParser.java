package org.infinispan.counter.configuration;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ConfigurationParser;
import org.infinispan.configuration.parsing.Namespace;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.configuration.parsing.Parser;
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
@Namespace(root = "weak-counter")
@Namespace(root = "strong-counter")
@Namespace(uri = Parser.NAMESPACE + "*", root = "weak-counter")
@Namespace(uri = Parser.NAMESPACE + "*", root = "strong-counter")
public class CounterParser implements ConfigurationParser {
   static final String NAMESPACE = Parser.NAMESPACE + "counters:";

   @Override
   public void readElement(ConfigurationReader reader, ConfigurationBuilderHolder holder) {
      GlobalConfigurationBuilder builder = holder.getGlobalConfigurationBuilder();
      readElement(reader, builder.addModule(CounterManagerConfigurationBuilder.class), Element.forName(reader.getLocalName()), null);
   }

   /**
    * Reads an element from the configuration.
    * @param reader The configuration reader.
    * @param builder The counter manager configuration builder.
    * @param element The element to read.
    * @param name The name of the counter.
    */
   public void readElement(ConfigurationReader reader, CounterManagerConfigurationBuilder builder, Element element, String name) {
      switch (element) {
         case STRONG_COUNTER:
            Schema schema = getSchema(reader);
            if (!schema.since(10, 0)) {
               parseStrongCounterLegacy(reader, builder.addStrongCounter().name(name));
            } else {
               parseStrongCounter(reader, builder.addStrongCounter().name(name));
            }
            break;
         case WEAK_COUNTER:
            parseWeakCounter(reader, builder.addWeakCounter().name(name));
            break;
         default:
            throw ParseUtils.unexpectedElement(reader);
      }
   }

   @Override
   public Namespace[] getNamespaces() {
      return ParseUtils.getNamespaceAnnotations(getClass());
   }

   private Schema getSchema(ConfigurationReader reader) {
      String namespaceURI = reader.getNamespace();
      if (namespaceURI == null)
         return new Schema(NAMESPACE, Integer.parseInt(Version.getMajor()), Integer.parseInt(Version.getMinor()));
      return Schema.fromNamespaceURI(namespaceURI);
   }

   /**
    * Parses a weak counter configuration.
    * @param reader The configuration reader.
    * @param builder The weak counter configuration builder.
    */
   private void parseWeakCounter(ConfigurationReader reader, WeakCounterConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         if (attribute == Attribute.CONCURRENCY_LEVEL) {
            builder.concurrencyLevel(Integer.parseInt(value));
         } else {
            parserCommonCounterAttributes(reader, builder, i, attribute, value);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   /**
    * Parses a strong counter configuration from a legacy schema.
    * @param reader The configuration reader.
    * @param builder The strong counter configuration builder.
    */
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

   /**
    * Parses the upper bound of a strong counter.
    * @param reader The configuration reader.
    * @param builder The strong counter configuration builder.
    */
   private void parseUpperBound(ConfigurationReader reader, StrongCounterConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         if (attribute != Attribute.VALUE) {
            throw ParseUtils.unexpectedElement(reader);
         }
         builder.upperBound(Long.parseLong(value));
      }
      ParseUtils.requireNoContent(reader);
   }

   /**
    * Parses the lower bound of a strong counter.
    * @param reader The configuration reader.
    * @param builder The strong counter configuration builder.
    */
   private void parseLowerBound(ConfigurationReader reader, StrongCounterConfigurationBuilder builder) {
      for (int i = 0; i < reader.getAttributeCount(); i++) {
         ParseUtils.requireNoNamespaceAttribute(reader, i);
         String value = reader.getAttributeValue(i);
         Attribute attribute = Attribute.forName(reader.getAttributeName(i));
         if (attribute != Attribute.VALUE) {
            throw ParseUtils.unexpectedElement(reader);
         }
         builder.lowerBound(Long.parseLong(value));
      }
      ParseUtils.requireNoContent(reader);
   }


   /**
    * Parses a strong counter configuration.
    * @param reader The configuration reader.
    * @param builder The strong counter configuration builder.
    */
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
            case LIFESPAN:
               builder.lifespan(Long.parseLong(value));
               break;
            default:
               parserCommonCounterAttributes(reader, builder, i, attribute, value);
         }
      }
      ParseUtils.requireNoContent(reader);
   }

   /**
    * Parses common counter attributes.
    * @param reader The configuration reader.
    * @param builder The counter configuration builder.
    * @param index The attribute index.
    * @param attribute The attribute.
    * @param value The attribute value.
    */
   private void parserCommonCounterAttributes(ConfigurationReader reader, CounterConfigurationBuilder<?,?> builder,
                                              int index, Attribute attribute, String value) {
      switch (attribute) {
         case NAME:
            // Already seen
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
