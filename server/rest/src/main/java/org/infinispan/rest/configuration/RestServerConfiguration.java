package org.infinispan.rest.configuration;

import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

@BuiltBy(RestServerConfigurationBuilder.class)
public class RestServerConfiguration extends ProtocolServerConfiguration {
   private final ExtendedHeaders extendedHeaders;

   RestServerConfiguration(ExtendedHeaders extendedHeaders, String host, int port, Set<String> ignoredCaches, SslConfiguration ssl) {
      super(null, null, host, port, -1, -1, -1, ssl, false, -1, ignoredCaches);
      this.extendedHeaders = extendedHeaders;
   }

   public ExtendedHeaders extendedHeaders() {
      return extendedHeaders;
   }

   /**
    * @deprecated Use {@link #ignoredCaches()} instead.
    */
   @Deprecated
   public Set<String> getIgnoredCaches() {
      return ignoredCaches();
   }
}
