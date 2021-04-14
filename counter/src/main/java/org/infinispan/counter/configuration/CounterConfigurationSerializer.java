package org.infinispan.counter.configuration;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.io.ConfigurationFormatFeature;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.parsing.Attribute;
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
      if (writer.hasFeature(ConfigurationFormatFeature.MIXED_ELEMENTS)) {
         writer.writeStartMap(Element.COUNTERS);
         writer.writeDefaultNamespace(CounterConfigurationParser.NAMESPACE + Version.getMajorMinor());
         configuration.attributes().write(writer);
         writeConfigurations(writer, configuration.counters().values());
         writer.writeEndMap();
      } else {
         writer.writeStartElement(Element.COUNTERS);
         writer.writeDefaultNamespace(CounterConfigurationParser.NAMESPACE + Version.getMajorMinor());
         configuration.attributes().write(writer);
         writer.writeStartMap(Element.COUNTERS);
         writeConfigurations(writer, configuration.counters().values());
         writer.writeEndMap();
         writer.writeEndElement();
      }
   }

   /**
    * It serializes a {@link List} of {@link AbstractCounterConfiguration} to an {@link OutputStream}.
    * @param os the {@link OutputStream} to write to.
    * @param configs the {@link List} if {@link AbstractCounterConfiguration}.
    */
   public void serializeConfigurations(OutputStream os, Collection<AbstractCounterConfiguration> configs) {
      BufferedOutputStream output = new BufferedOutputStream(os);
      ConfigurationWriter writer = ConfigurationWriter.to(output).build();
      writer.writeStartDocument();
      writer.writeStartMap(Element.COUNTERS);
      writeConfigurations(writer, configs);
      writer.writeEndMap();
      writer.writeEndDocument();
      Util.close(writer);
   }

   /**
    * Serializes a single counter configuration
    * @param writer
    * @param c
    */
   public void serializeConfiguration(ConfigurationWriter writer, AbstractCounterConfiguration c) {
      writer.writeStartDocument();
      writeConfiguration(writer, c, true);
      writer.writeEndDocument();
   }

   private void writeConfigurations(ConfigurationWriter writer, Collection<AbstractCounterConfiguration> configs) {
      for (AbstractCounterConfiguration c : configs) {
         writeConfiguration(writer, c, false);
      }
   }

   private void writeConfiguration(ConfigurationWriter writer, AbstractCounterConfiguration c, boolean unnamed) {
      if (c instanceof StrongCounterConfiguration) {
         writeStrongConfiguration((StrongCounterConfiguration) c, writer, unnamed);
      } else if (c instanceof WeakCounterConfiguration) {
         writeWeakConfiguration((WeakCounterConfiguration) c, writer, unnamed);
      }
   }

   private void writeWeakConfiguration(WeakCounterConfiguration configuration, ConfigurationWriter writer, boolean unnamed) {
      if (unnamed) {
         writer.writeStartElement(Element.WEAK_COUNTER);
      } else {
         writer.writeMapItem(Element.WEAK_COUNTER, Attribute.NAME, configuration.name());
      }
      configuration.attributes().write(writer);
      if (unnamed) {
         writer.writeEndElement();
      } else {
         writer.writeEndMapItem();
      }
   }

   private void writeStrongConfiguration(StrongCounterConfiguration configuration, ConfigurationWriter writer, boolean unnamed) {
      if (unnamed) {
         writer.writeStartElement(Element.STRONG_COUNTER);
      } else {
         writer.writeMapItem(Element.STRONG_COUNTER, Attribute.NAME, configuration.name());
      }
      configuration.attributes().write(writer);
      if (unnamed) {
         writer.writeEndElement();
      } else {
         writer.writeEndMapItem();
      }
   }
}
