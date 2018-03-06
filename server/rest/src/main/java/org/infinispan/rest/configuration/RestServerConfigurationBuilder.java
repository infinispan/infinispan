package org.infinispan.rest.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.rest.logging.Log;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.util.logging.LogFactory;

/**
 * RestServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class RestServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<RestServerConfiguration, RestServerConfigurationBuilder> implements
      Builder<RestServerConfiguration> {

   private final static Log logger = LogFactory.getLog(RestServerConfigurationBuilder.class, Log.class);

   public static final String DEFAULT_CONTEXT_PATH = "rest";
   public static final int DEFAULT_PORT = 8080;
   public static final String DEFAULT_NAME = "rest";
   public static final int DEFAULT_MAX_CONTENT_LENGTH = 10 * 1024 * 1024;

   private ExtendedHeaders extendedHeaders = ExtendedHeaders.ON_DEMAND;
   private List<String> corsAllowOrigins = new ArrayList<>(6);
   private String contextPath = DEFAULT_CONTEXT_PATH;
   private int maxContentLength = DEFAULT_MAX_CONTENT_LENGTH;

   public RestServerConfigurationBuilder() {
      super(DEFAULT_PORT);
      name(DEFAULT_NAME);
   }

   public RestServerConfigurationBuilder extendedHeaders(ExtendedHeaders extendedHeaders) {
      this.extendedHeaders = extendedHeaders;
      return this;
   }

   public RestServerConfigurationBuilder contextPath(String contextPath) {
      this.contextPath = contextPath;
      return this;
   }

   public RestServerConfigurationBuilder maxContentLength(int maxContentLength) {
      this.maxContentLength = maxContentLength;
      return this;
   }

   public RestServerConfigurationBuilder corsAllowForLocalhost(String scheme, int port) {
      this.corsAllowOrigins.add(scheme + "://" + "127.0.0.1" + ":" + port);
      this.corsAllowOrigins.add(scheme + "://" + "localhost" + ":" + port);
      this.corsAllowOrigins.add(scheme + "://" + "[::1]" + ":" + port);
      return this;
   }

   @Override
   public void validate() {
      // Nothing to do
   }

   @Override
   public RestServerConfiguration create() {
      return new RestServerConfiguration(defaultCacheName, name, extendedHeaders, host, port, ignoredCaches, ssl.create(),
            startTransport, contextPath, adminOperationsHandler, maxContentLength, corsAllowOrigins);
   }

   @Override
   public Builder<?> read(RestServerConfiguration template) {
      this.extendedHeaders = template.extendedHeaders();
      this.host = template.host();
      this.port = template.port();
      this.maxContentLength = template.maxContentLength();
      this.corsAllowOrigins = template.getCorsAllowOrigins();
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

   @Override
   public RestServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public RestServerConfigurationBuilder defaultCacheName(String defaultCacheName) {
      throw logger.unsupportedConfigurationOption();
   }

   @Override
   public RestServerConfigurationBuilder idleTimeout(int idleTimeout) {
      throw logger.unsupportedConfigurationOption();
   }

   @Override
   public RestServerConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      throw logger.unsupportedConfigurationOption();
   }

   @Override
   public RestServerConfigurationBuilder recvBufSize(int recvBufSize) {
      throw logger.unsupportedConfigurationOption();
   }

   @Override
   public RestServerConfigurationBuilder sendBufSize(int sendBufSize) {
      throw logger.unsupportedConfigurationOption();
   }

   @Override
   public RestServerConfigurationBuilder workerThreads(int workerThreads) {
      throw logger.unsupportedConfigurationOption();
   }
}
