package org.infinispan.client.openapi.configuration;

import java.util.Properties;

/**
 * AbstractConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 16.0
 */
public abstract class AbstractConfigurationChildBuilder implements OpenAPIClientConfigurationChildBuilder {
   final OpenAPIClientConfigurationBuilder builder;

   protected AbstractConfigurationChildBuilder(OpenAPIClientConfigurationBuilder builder) {
      this.builder = builder;
   }

   @Override
   public ServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

   @Override
   public OpenAPIClientConfigurationBuilder addServers(String servers) {
      return builder.addServers(servers);
   }

   @Override
   public OpenAPIClientConfigurationBuilder protocol(Protocol protocol) {
      return builder.protocol(protocol);
   }

   @Override
   public OpenAPIClientConfigurationBuilder connectionTimeout(long connectionTimeout) {
      return builder.connectionTimeout(connectionTimeout);
   }

   @Override
   public OpenAPIClientConfigurationBuilder priorKnowledge(boolean enabled) {
      return builder.priorKnowledge(enabled);
   }

   @Override
   public OpenAPIClientConfigurationBuilder followRedirects(boolean followRedirects) {
      return builder.followRedirects(followRedirects);
   }

   @Override
   public OpenAPIClientConfigurationBuilder pingOnCreate(boolean pingOnCreate) {
      return builder.pingOnCreate(pingOnCreate);
   }

   @Override
   public OpenAPIClientConfigurationBuilder socketTimeout(long socketTimeout) {
      return builder.socketTimeout(socketTimeout);
   }

   @Override
   public SecurityConfigurationBuilder security() {
      return builder.security();
   }

   @Override
   public OpenAPIClientConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      return builder.tcpNoDelay(tcpNoDelay);
   }

   @Override
   public OpenAPIClientConfigurationBuilder tcpKeepAlive(boolean tcpKeepAlive) {
      return builder.tcpKeepAlive(tcpKeepAlive);
   }

   @Override
   public OpenAPIClientConfigurationBuilder withProperties(Properties properties) {
      return builder.withProperties(properties);
   }

   @Override
   public OpenAPIClientConfiguration build() {
      return builder.build();
   }

}
