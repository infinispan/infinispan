package org.infinispan.client.openapi.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * ServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 16.0
 */
public class ServerConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ServerConfiguration> {
   private String host;
   private int port = OpenAPIClientConfigurationProperties.DEFAULT_REST_PORT;

   ServerConfigurationBuilder(OpenAPIClientConfigurationBuilder builder) {
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
         throw new IllegalStateException("Missing host definition");
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
