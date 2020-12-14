package org.infinispan.configuration.internal;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

/**
 * A {@link ConfigurationSerializer} implementation for {@link PrivateGlobalConfiguration}.
 * <p>
 * The {@link PrivateGlobalConfiguration} is only to be used internally so this implementation is a no-op.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class PrivateGlobalConfigurationSerializer implements ConfigurationSerializer<PrivateGlobalConfiguration> {
   @Override
   public void serialize(ConfigurationWriter writer, PrivateGlobalConfiguration configuration) {
      //nothing to do! private configuration is not serialized.
   }
}
