package org.infinispan.metrics.config;

import java.util.Objects;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;

/**
 * Builder to inject an instance of {@link MeterRegistry}.
 * <p>
 * If not configured, Infinispan will create a new instance of {@link PrometheusMeterRegistry}.
 *
 * @since 15.0
 */
public class MicrometerMeterRegisterConfigurationBuilder implements Builder<MicrometerMeterRegistryConfiguration> {

   private MeterRegistry meterRegistry;

   public MicrometerMeterRegisterConfigurationBuilder(GlobalConfigurationBuilder builder) {
      //required because GlobalConfigurationBuilder#addModule uses reflection
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   /**
    * Set the {@link MeterRegistry} instance to use by Infinispan.
    * <p>
    * If set to {@code null}, Infinispan will create a new instance of {@link PrometheusMeterRegistry}.
    *
    * @param registry The {@link MeterRegistry} to use or {@code null}.
    */
   public MicrometerMeterRegisterConfigurationBuilder meterRegistry(MeterRegistry registry) {
      meterRegistry = registry;
      return this;
   }

   @Override
   public MicrometerMeterRegistryConfiguration create() {
      return new MicrometerMeterRegistryConfiguration(meterRegistry);
   }

   @Override
   public MicrometerMeterRegisterConfigurationBuilder read(MicrometerMeterRegistryConfiguration template, Combine combine) {
      meterRegistry(template.meterRegistry());
      return this;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MicrometerMeterRegisterConfigurationBuilder)) return false;

      MicrometerMeterRegisterConfigurationBuilder that = (MicrometerMeterRegisterConfigurationBuilder) o;

      return Objects.equals(meterRegistry, that.meterRegistry);
   }

   @Override
   public int hashCode() {
      return meterRegistry != null ? meterRegistry.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "MicrometerMeterRegisterConfigurationBuilder{" +
            "meterRegistry=" + meterRegistry +
            '}';
   }
}
