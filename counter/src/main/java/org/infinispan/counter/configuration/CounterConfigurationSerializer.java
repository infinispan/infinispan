package org.infinispan.counter.configuration;

import javax.xml.stream.XMLStreamException;

import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;

/**
 * Counters configuration serializer.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterConfigurationSerializer implements ConfigurationSerializer<CounterManagerConfiguration> {

   @Override
   public void serialize(XMLExtendedStreamWriter writer, CounterManagerConfiguration configuration)
         throws XMLStreamException {
      writer.writeStartElement(Element.COUNTERS);
      configuration.attributes().write(writer);
      for (AbstractCounterConfiguration c : configuration.counters()) {
         if (c instanceof StrongCounterConfiguration) {
            writeStrongConfiguration((StrongCounterConfiguration) c, writer);
         } else if (c instanceof WeakCounterConfiguration) {
            writeWeakConfiguration((WeakCounterConfiguration) c, writer);
         }
      }
      writer.writeEndElement();
   }

   private void writeWeakConfiguration(WeakCounterConfiguration configuration, XMLExtendedStreamWriter writer)
         throws XMLStreamException {
      writer.writeStartElement(Element.WEAK_COUNTER);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }

   private void writeStrongConfiguration(StrongCounterConfiguration configuration, XMLExtendedStreamWriter writer)
         throws XMLStreamException {
      writer.writeStartElement(Element.STRONG_COUNTER);
      configuration.attributes().write(writer);
      if (configuration.attributes().attribute(StrongCounterConfiguration.LOWER_BOUND).isModified()) {
         writeBound(Element.LOWER_BOUND, configuration.lowerBound(), writer);
      }
      if (configuration.attributes().attribute(StrongCounterConfiguration.UPPER_BOUND).isModified()) {
         writeBound(Element.UPPER_BOUND, configuration.upperBound(), writer);
      }
      writer.writeEndElement();
   }

   private void writeBound(Element element, long value, XMLExtendedStreamWriter writer) throws XMLStreamException {
      writer.writeStartElement(element);
      writer.writeAttribute(Attribute.VALUE, Long.toString(value));
      writer.writeEndElement();
   }
}
