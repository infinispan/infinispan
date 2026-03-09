package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * Configuration for the memory monitor. All attributes are mutable at runtime.
 *
 * @since 16.2
 */
public class GlobalMemoryMonitorConfiguration extends ConfigurationElement<GlobalMemoryMonitorConfiguration> {

   public static final AttributeDefinition<Boolean> ENABLED =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, true).global(false).build();

   public static final AttributeDefinition<Double> MEMORY_THRESHOLD =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MEMORY_THRESHOLD, 0.85).global(false).build();

   public static final AttributeDefinition<Long> GC_DURATION_THRESHOLD =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.GC_DURATION_THRESHOLD, 5000L).global(false).build();

   public static final AttributeDefinition<Double> GC_PRESSURE_THRESHOLD =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.GC_PRESSURE_THRESHOLD, 0.20).global(false).build();

   public static final AttributeDefinition<Long> GC_PRESSURE_WINDOW =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.GC_PRESSURE_WINDOW, 60_000L).global(false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalMemoryMonitorConfiguration.class,
            ENABLED, MEMORY_THRESHOLD, GC_DURATION_THRESHOLD, GC_PRESSURE_THRESHOLD, GC_PRESSURE_WINDOW);
   }

   GlobalMemoryMonitorConfiguration(AttributeSet attributes) {
      super(Element.MEMORY_MONITOR, attributes);
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public double memoryThreshold() {
      return attributes.attribute(MEMORY_THRESHOLD).get();
   }

   public long gcDurationThreshold() {
      return attributes.attribute(GC_DURATION_THRESHOLD).get();
   }

   public double gcPressureThreshold() {
      return attributes.attribute(GC_PRESSURE_THRESHOLD).get();
   }

   public long gcPressureWindow() {
      return attributes.attribute(GC_PRESSURE_WINDOW).get();
   }
}
