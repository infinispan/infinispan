package org.infinispan.api.client.configuration;

import java.util.Properties;

import org.infinispan.api.configuration.ClientConfig;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

public class InfinispanClientConfigImpl implements ClientConfig {

   private final Configuration configuration;

   public InfinispanClientConfigImpl(Properties properties) {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.withProperties(properties);
      configuration = configurationBuilder.build();
   }

   public Configuration getConfiguration() {
      return configuration;
   }
}
