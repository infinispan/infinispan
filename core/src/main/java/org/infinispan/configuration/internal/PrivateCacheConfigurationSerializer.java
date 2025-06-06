package org.infinispan.configuration.internal;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.serializing.ConfigurationSerializer;

public class PrivateCacheConfigurationSerializer implements ConfigurationSerializer<PrivateCacheConfiguration> {
   @Override
   public void serialize(ConfigurationWriter writer, PrivateCacheConfiguration configuration) {
      //nothing to do! private configuration is not serialized.
   }
}
