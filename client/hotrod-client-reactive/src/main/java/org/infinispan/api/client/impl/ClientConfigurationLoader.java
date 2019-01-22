package org.infinispan.api.client.impl;

import org.infinispan.api.ClientConfig;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

/**
 *
 * @since 10.0
 */
public class ClientConfigurationLoader {

   private static final Configuration defaultConfiguration = new ConfigurationBuilder().build();

   /**
    * Retrieves the default configuration
    *
    * @return ClientConfig default configuration
    */
   public static ClientConfig defaultClientConfig() {
      return new ConfigurationWrapper(defaultConfiguration);
   }

   static public class ConfigurationWrapper implements ClientConfig {

      private Configuration configuration;

      public ConfigurationWrapper(Configuration configuration) {
         this.configuration = configuration;
      }

      public Configuration getConfiguration(){
         return configuration;
      }
   }
}
