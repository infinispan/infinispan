package org.infinispan.metrics.config;

import java.util.Objects;

import org.infinispan.configuration.serializing.SerializedWith;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * A configuration class to inject a custom {@link MeterRegistry} instance.
 *
 * @since 15.0
 */
@SerializedWith(MicrometerMeterRegistryConfigurationSerializer.class)
public class MicrometerMeterRegistryConfiguration {

   private final MeterRegistry registry;

   public MicrometerMeterRegistryConfiguration(MeterRegistry registry) {
      this.registry = registry;
   }

   /**
    * @return The {@link MeterRegistry} instance injected or {@code null} if not configured.
    */
   public MeterRegistry meterRegistry() {
      return registry;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MicrometerMeterRegistryConfiguration)) return false;

      MicrometerMeterRegistryConfiguration that = (MicrometerMeterRegistryConfiguration) o;

      return Objects.equals(registry, that.registry);
   }

   @Override
   public int hashCode() {
      return registry != null ? registry.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "MicrometerMeterRegistryConfiguration{" +
            "registry=" + registry +
            '}';
   }
}
