package org.infinispan.api.client.impl;

import org.infinispan.api.ClientConfig;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;

/**
 * @since 10.0
 */
public class ClientConfigurationLoader {

   static final ConfigurationBuilder DEFAULT_CONFIGURATION_BUILDER = new ConfigurationBuilder()
         .addServer().host("127.0.0.1").port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
         .marshaller(new ProtoStreamMarshaller());

   /**
    * Retrieves the default configuration
    *
    * @return ClientConfig default configuration
    */
   public static ClientConfig defaultClientConfig() {
      return new ConfigurationWrapper(DEFAULT_CONFIGURATION_BUILDER.build());
   }

   static public class ConfigurationWrapper implements ClientConfig {

      private Configuration configuration;

      public ConfigurationWrapper(Configuration configuration) {
         this.configuration = configuration;
      }

      public Configuration getConfiguration() {
         return configuration;
      }
   }
}
