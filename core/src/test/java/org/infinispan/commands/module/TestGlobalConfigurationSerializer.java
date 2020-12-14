package org.infinispan.commands.module;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

/**
 * A {@link ConfigurationSerializer} implementation for {@link TestGlobalConfiguration}.
 * <p>
 * The {@link TestGlobalConfiguration} is only to be used internally so this implementation is a no-op.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TestGlobalConfigurationSerializer implements ConfigurationSerializer<TestGlobalConfiguration> {
   @Override
   public void serialize(ConfigurationWriter writer, TestGlobalConfiguration configuration) {
      //nothing to do! test configuration is not serialized.
   }
}
