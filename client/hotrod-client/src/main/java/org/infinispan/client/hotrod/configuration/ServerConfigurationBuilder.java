package org.infinispan.client.hotrod.configuration;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;

/**
 * ServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ServerConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ServerConfiguration> {

   private static final Log log = LogFactory.getLog(ServerConfigurationBuilder.class);

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
      if (host == null || host.isEmpty()) {
         throw log.missingHostDefinition();
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
