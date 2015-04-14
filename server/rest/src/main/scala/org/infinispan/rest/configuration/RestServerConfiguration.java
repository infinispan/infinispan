package org.infinispan.rest.configuration;

import org.infinispan.commons.configuration.BuiltBy;

@BuiltBy(RestServerConfigurationBuilder.class)
public class RestServerConfiguration {
   private final ExtendedHeaders extendedHeaders;
   private final String host;
   private final int port;

   RestServerConfiguration(ExtendedHeaders extendedHeaders, String host, int port) {
      this.extendedHeaders = extendedHeaders;
      this.host = host;
      this.port = port;
   }

   public ExtendedHeaders extendedHeaders() {
      return extendedHeaders;
   }

   public int port() {
      return port;
   }

   public String host() {
      return host;
   }
}
