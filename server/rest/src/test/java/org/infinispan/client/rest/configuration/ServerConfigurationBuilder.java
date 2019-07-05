package org.infinispan.client.rest.configuration;

import org.infinispan.commons.configuration.Builder;

/**
 * ServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class ServerConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ServerConfiguration> {
   private String host;
   private int port = RestClientConfigurationProperties.DEFAULT_REST_PORT;

   ServerConfigurationBuilder(RestClientConfigurationBuilder builder) {
      super(builder);
   }

   public ServerConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   public ServerConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   @Override
   public void validate() {
      if (host == null || host.isEmpty()) {
         throw new IllegalStateException("Missing host definition");
      }
   }

   @Override
   public ServerConfiguration create() {
      return new ServerConfiguration(host, port);
   }

   @Override
   public ServerConfigurationBuilder read(ServerConfiguration template) {
      this.host = template.host();
      this.port = template.port();

      return this;
   }

}
