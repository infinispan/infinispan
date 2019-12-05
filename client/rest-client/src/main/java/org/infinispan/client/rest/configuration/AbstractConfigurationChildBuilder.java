package org.infinispan.client.rest.configuration;

import java.util.Properties;

/**
 * AbstractConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public abstract class AbstractConfigurationChildBuilder implements RestClientConfigurationChildBuilder {
   final RestClientConfigurationBuilder builder;

   protected AbstractConfigurationChildBuilder(RestClientConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public ServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

   @Override
   public RestClientConfigurationBuilder addServers(String servers) {
      return builder.addServers(servers);
   }

   @Override
   public RestClientConfigurationBuilder protocol(Protocol protocol) {
      return builder.protocol(protocol);
   }

   @Override
   public RestClientConfigurationBuilder connectionTimeout(long connectionTimeout) {
      return builder.connectionTimeout(connectionTimeout);
   }

   @Override
   public RestClientConfigurationBuilder priorKnowledge(boolean enabled) {
      return builder.priorKnowledge(enabled);
   }

   @Override
   public RestClientConfigurationBuilder followRedirects(boolean followRedirects) {
      return builder.followRedirects(followRedirects);
   }

   @Override
   public RestClientConfigurationBuilder socketTimeout(long socketTimeout) {
      return builder.socketTimeout(socketTimeout);
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return builder.security();
   }

   @Override
   public RestClientConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      return builder.tcpNoDelay(tcpNoDelay);
   }

   @Override
   public RestClientConfigurationBuilder tcpKeepAlive(boolean tcpKeepAlive) {
      return builder.tcpKeepAlive(tcpKeepAlive);
   }

   @Override
   public RestClientConfigurationBuilder withProperties(Properties properties) {
      return builder.withProperties(properties);
   }

   @Override
   public RestClientConfiguration build() {
      return builder.build();
   }

}
