package org.infinispan.counter.configuration;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.List;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

/**
 * Counters configuration serializer.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterConfigurationSerializer implements ConfigurationSerializer<CounterManagerConfiguration> {

   @Override
   public void serialize(ConfigurationWriter writer, CounterManagerConfiguration configuration) {
      writer.writeStartListElement(Element.COUNTERS, true);
      writer.writeDefaultNamespace(CounterConfigurationParser.NAMESPACE + Version.getMajorMinor());
      configuration.attributes().write(writer);
      writeConfigurations(writer, configuration.counters());
      writer.writeEndListElement();
   }

   /**
    * It serializes a {@link List} of {@link AbstractCounterConfiguration} to an {@link OutputStream}.
    * @param os the {@link OutputStream} to write to.
    * @param configs the {@link List} if {@link AbstractCounterConfiguration}.
    */
   public void serializeConfigurations(OutputStream os, List<AbstractCounterConfiguration> configs) {
      BufferedOutputStream output = new BufferedOutputStream(os);
      ConfigurationWriter writer = ConfigurationWriter.to(output).build();
      writer.writeStartDocument();
      writer.writeStartElement(Element.COUNTERS);
      writeConfigurations(writer, configs);

      writer.writeEndElement();
      writer.writeEndDocument();
      Util.close(writer);
   }

   private void writeConfigurations(ConfigurationWriter writer, List<AbstractCounterConfiguration> configs) {
      for (AbstractCounterConfiguration c : configs) {
         if (c instanceof StrongCounterConfiguration) {
            writeStrongConfiguration((StrongCounterConfiguration) c, writer);
         } else if (c instanceof WeakCounterConfiguration) {
            writeWeakConfiguration((WeakCounterConfiguration) c, writer);
         }
      }
   }

   private void writeWeakConfiguration(WeakCounterConfiguration configuration, ConfigurationWriter writer) {
      writer.writeStartElement(Element.WEAK_COUNTER);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }

   private void writeStrongConfiguration(StrongCounterConfiguration configuration, ConfigurationWriter writer) {
      writer.writeStartElement(Element.STRONG_COUNTER);
      configuration.attributes().write(writer);
      writer.writeEndElement();
   }
}
