package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

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

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
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
         throw HOTROD.missingHostDefinition();
      }
   }

   @Override
   public ServerConfiguration create() {
      return new ServerConfiguration(host, port);
   }

   @Override
   public ServerConfigurationBuilder read(ServerConfiguration template, Combine combine) {
      this.host = template.host();
      this.port = template.port();

      return this;
   }

}
