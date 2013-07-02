package org.infinispan.client.hotrod.configuration;

import org.infinispan.commons.configuration.Builder;

/**
 * ServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ServerConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ServerConfiguration> {
   private String host;
   private int port = 11222;

   ServerConfigurationBuilder(ConfigurationBuilder builder) {
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
