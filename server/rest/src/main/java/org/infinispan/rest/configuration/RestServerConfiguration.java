package org.infinispan.rest.configuration;

import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

@BuiltBy(RestServerConfigurationBuilder.class)
public class RestServerConfiguration extends ProtocolServerConfiguration {
   private final ExtendedHeaders extendedHeaders;
   private final String contextPath;
   private final int maxContentLength;

   RestServerConfiguration(String defaultCacheName, String name, ExtendedHeaders extendedHeaders, String host, int port,
                           Set<String> ignoredCaches, SslConfiguration ssl, boolean startTransport, String contextPath,
                           AdminOperationsHandler adminOperationsHandler, int maxContentLength) {
      super(defaultCacheName, name, host, port, -1, -1, -1, ssl, false,
            -1, ignoredCaches, startTransport, adminOperationsHandler);
      this.extendedHeaders = extendedHeaders;
      this.contextPath = contextPath;
      this.maxContentLength = maxContentLength;
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

   public String contextPath() {
      return contextPath;
   }

   public int maxContentLength() {
      return maxContentLength;
   }
}
