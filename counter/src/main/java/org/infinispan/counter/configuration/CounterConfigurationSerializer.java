package org.infinispan.counter.configuration;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.infinispan.configuration.serializing.ConfigurationSerializer;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriter;
import org.infinispan.configuration.serializing.XMLExtendedStreamWriterImpl;

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
      writeConfigurations(writer, configuration.counters());
      writer.writeEndElement();
   }

   /**
    * It serializes a {@link List} of {@link AbstractCounterConfiguration} to an {@link OutputStream}.
    * @param os the {@link OutputStream} to write to.
    * @param configs the {@link List} if {@link AbstractCounterConfiguration}.
    * @throws XMLStreamException if xml is malformed
    */
   public void serializeConfigurations(OutputStream os, List<AbstractCounterConfiguration> configs)
         throws XMLStreamException {
      BufferedOutputStream output = new BufferedOutputStream(os);
      XMLStreamWriter subWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(output);
      XMLExtendedStreamWriter writer = new XMLExtendedStreamWriterImpl(subWriter);
      writer.writeStartDocument();
      writer.writeStartElement(Element.COUNTERS);
      writeConfigurations(writer, configs);
      writer.writeEndElement();
      writer.writeEndDocument();
      subWriter.close();
   }

   private void writeConfigurations(XMLExtendedStreamWriter writer, List<AbstractCounterConfiguration> configs)
         throws XMLStreamException {
      for (AbstractCounterConfiguration c : configs) {
         if (c instanceof StrongCounterConfiguration) {
            writeStrongConfiguration((StrongCounterConfiguration) c, writer);
         } else if (c instanceof WeakCounterConfiguration) {
            writeWeakConfiguration((WeakCounterConfiguration) c, writer);
         }
      }
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
