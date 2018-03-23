package org.infinispan.rest.configuration;

import java.util.List;
import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.admin.AdminOperationsHandler;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

import io.netty.handler.codec.http.cors.CorsConfig;

@BuiltBy(RestServerConfigurationBuilder.class)
public class RestServerConfiguration extends ProtocolServerConfiguration {
   private final ExtendedHeaders extendedHeaders;
   private final String contextPath;
   private final int maxContentLength;
   private final List<CorsConfig> corsRules;
   private final int compressionLevel;

   RestServerConfiguration(String defaultCacheName, String name, ExtendedHeaders extendedHeaders, String host, int port,
                           Set<String> ignoredCaches, SslConfiguration ssl, boolean startTransport, String contextPath,
                           AdminOperationsHandler adminOperationsHandler, int maxContentLength,
                           List<CorsConfig> corsRules, int compressionLevel) {
      super(defaultCacheName, name, host, port, -1, -1, -1, ssl, false,
            -1, ignoredCaches, startTransport, adminOperationsHandler);
      this.extendedHeaders = extendedHeaders;
      this.contextPath = contextPath;
      this.maxContentLength = maxContentLength;
      this.corsRules = corsRules;
      this.compressionLevel = compressionLevel;
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

   public List<CorsConfig> getCorsRules() {
      return corsRules;
   }

   public int getCompressionLevel() {
      return compressionLevel;
   }
}
