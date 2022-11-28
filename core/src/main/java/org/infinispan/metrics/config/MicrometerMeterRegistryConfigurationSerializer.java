package org.infinispan.metrics.config;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

/**
 * A {@link ConfigurationSerializer} implementation to serialize {@link MicrometerMeterRegistryConfiguration}.
 * <p>
 * This class is a no-op.
 *
 * @since 15.0
 */
public class MicrometerMeterRegistryConfigurationSerializer implements ConfigurationSerializer<MicrometerMeterRegistryConfiguration> {
   @Override
   public void serialize(ConfigurationWriter writer, MicrometerMeterRegistryConfiguration configuration) {
      //no-op
   }
}
