package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalMemoryMonitorConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalMemoryMonitorConfiguration.GC_DURATION_THRESHOLD;
import static org.infinispan.configuration.global.GlobalMemoryMonitorConfiguration.GC_PRESSURE_THRESHOLD;
import static org.infinispan.configuration.global.GlobalMemoryMonitorConfiguration.GC_PRESSURE_WINDOW;
import static org.infinispan.configuration.global.GlobalMemoryMonitorConfiguration.MEMORY_THRESHOLD;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Builder for {@link GlobalMemoryMonitorConfiguration}.
 *
 * @since 16.2
 */
public class GlobalMemoryMonitorConfigurationBuilder extends AbstractGlobalConfigurationBuilder
      implements Builder<GlobalMemoryMonitorConfiguration> {

   private final AttributeSet attributes;

   GlobalMemoryMonitorConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = GlobalMemoryMonitorConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Enables or disables the memory monitor. When disabled, no JMX listeners are registered
    * and no alerts are raised. Default is {@code true}.
    *
    * @param enabled whether the memory monitor is enabled
    */
   public GlobalMemoryMonitorConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Sets the fraction of old generation heap usage that triggers a low memory alert.
    * Must be between 0 (exclusive) and 1.0 (inclusive). Default is 0.85.
    *
    * @param memoryThreshold the memory threshold as a fraction
    */
   public GlobalMemoryMonitorConfigurationBuilder memoryThreshold(double memoryThreshold) {
      attributes.attribute(MEMORY_THRESHOLD).set(memoryThreshold);
      return this;
   }

   /**
    * Sets the GC pause duration in milliseconds that triggers a GC duration alert.
    * A single GC pause exceeding this value will raise the alert. Default is 5000.
    *
    * @param gcDurationThreshold the GC duration threshold in milliseconds
    */
   public GlobalMemoryMonitorConfigurationBuilder gcDurationThreshold(long gcDurationThreshold) {
      attributes.attribute(GC_DURATION_THRESHOLD).set(gcDurationThreshold);
      return this;
   }

   /**
    * Sets the fraction of time spent in GC over the pressure window that triggers a GC pressure alert.
    * Must be between 0 (exclusive) and 1.0 (inclusive). Default is 0.20.
    *
    * @param gcPressureThreshold the GC pressure threshold as a fraction
    */
   public GlobalMemoryMonitorConfigurationBuilder gcPressureThreshold(double gcPressureThreshold) {
      attributes.attribute(GC_PRESSURE_THRESHOLD).set(gcPressureThreshold);
      return this;
   }

   /**
    * Sets the rolling time window in milliseconds over which GC pressure is calculated.
    * Must be positive. Default is 60000.
    *
    * @param gcPressureWindow the GC pressure window in milliseconds
    */
   public GlobalMemoryMonitorConfigurationBuilder gcPressureWindow(long gcPressureWindow) {
      attributes.attribute(GC_PRESSURE_WINDOW).set(gcPressureWindow);
      return this;
   }

   @Override
   public GlobalMemoryMonitorConfiguration create() {
      return new GlobalMemoryMonitorConfiguration(attributes.protect());
   }

   @Override
   public GlobalMemoryMonitorConfigurationBuilder read(GlobalMemoryMonitorConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "GlobalMemoryMonitorConfigurationBuilder{attributes=" + attributes + '}';
   }
}
