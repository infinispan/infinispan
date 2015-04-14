package org.infinispan.rest.configuration;

import org.infinispan.commons.configuration.Builder;

/**
 * RestServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class RestServerConfigurationBuilder implements Builder<RestServerConfiguration> {

   private ExtendedHeaders extendedHeaders = ExtendedHeaders.ON_DEMAND;
   private int port = 8080;
   private String host = "localhost";

   public RestServerConfigurationBuilder extendedHeaders(ExtendedHeaders extendedHeaders) {
      this.extendedHeaders = extendedHeaders;
      return this;
   }

   public RestServerConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   public RestServerConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   @Override
   public void validate() {
      // Nothing to do
   }

   @Override
   public RestServerConfiguration create() {
      return new RestServerConfiguration(extendedHeaders, host, port);
   }

   @Override
   public Builder<?> read(RestServerConfiguration template) {
      this.extendedHeaders = template.extendedHeaders();
      this.host = template.host();
      this.port = template.port();
      return this;
   }

   public RestServerConfiguration build() {
      return build(true);
   }

   public RestServerConfiguration build(boolean validate) {
      if (validate)
         validate();
      return create();
   }

}
