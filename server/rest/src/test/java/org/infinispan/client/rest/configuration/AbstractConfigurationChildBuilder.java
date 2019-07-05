package org.infinispan.client.rest.configuration;

import java.util.Properties;

/**
 * AbstractConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public abstract class AbstractConfigurationChildBuilder implements ConfigurationChildBuilder {
   final ConfigurationBuilder builder;

   protected AbstractConfigurationChildBuilder(ConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public ServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

   @Override
   public ConfigurationBuilder addServers(String servers) {
      return builder.addServers(servers);
   }

   @Override
   public ConfigurationBuilder protocol(Protocol protocol) {
      return builder.protocol(protocol);
   }

   @Override
   public ConfigurationBuilder connectionTimeout(int connectionTimeout) {
      return builder.connectionTimeout(connectionTimeout);
   }

   @Override
   public ConfigurationBuilder socketTimeout(int socketTimeout) {
      return builder.socketTimeout(socketTimeout);
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return builder.security();
   }

   @Override
   public ConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      return builder.tcpNoDelay(tcpNoDelay);
   }

   @Override
   public ConfigurationBuilder tcpKeepAlive(boolean tcpKeepAlive) {
      return builder.tcpKeepAlive(tcpKeepAlive);
   }

   @Override
   public ConfigurationBuilder withProperties(Properties properties) {
      return builder.withProperties(properties);
   }

   @Override
   public Configuration build() {
      return builder.build();
   }

}
