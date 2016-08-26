package org.infinispan.rest.configuration;

import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;

@BuiltBy(RestServerConfigurationBuilder.class)
public class RestServerConfiguration {
   private final ExtendedHeaders extendedHeaders;
   private final String host;
   private final int port;
   private Set<String> ignoredCaches;

   RestServerConfiguration(ExtendedHeaders extendedHeaders, String host, int port, Set<String> ignoredCaches) {
      this.extendedHeaders = extendedHeaders;
      this.host = host;
      this.port = port;
      this.ignoredCaches = ignoredCaches;
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

   public Set<String> getIgnoredCaches() {
      return ignoredCaches;
   }
}
