package org.infinispan.rest.configuration;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.rest.logging.Log;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;

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
   public static final int DEFAULT_COMPRESS_LEVEL = 6;

   private ExtendedHeaders extendedHeaders = ExtendedHeaders.ON_DEMAND;
   private List<CorsConfig> corsRules = new ArrayList<>(3);
   private String contextPath = DEFAULT_CONTEXT_PATH;
   private int maxContentLength = DEFAULT_MAX_CONTENT_LENGTH;
   private int compressionLevel = DEFAULT_COMPRESS_LEVEL;

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

   public RestServerConfigurationBuilder compressionLevel(int compressLevel) {
      this.compressionLevel = compressLevel;
      return this;
   }

   public RestServerConfigurationBuilder corsAllowForLocalhost(String scheme, int port) {
      String local1 = scheme + "://" + "127.0.0.1" + ":" + port;
      String local2 = scheme + "://" + "localhost" + ":" + port;
      String local3 = scheme + "://" + "[::1]" + ":" + port;
      CorsConfig corsConfig = CorsConfigBuilder.forOrigins(local1, local2, local3)
            .allowCredentials()
            .allowedRequestMethods(GET, POST, PUT, DELETE, HEAD, OPTIONS)
            .allowedRequestHeaders(HttpHeaderNames.CONTENT_TYPE)
            .build();
      corsRules.add(corsConfig);
      return this;
   }

   public RestServerConfigurationBuilder addAll(List<CorsConfig> corsConfig) {
      corsRules.addAll(corsConfig);
      return this;
   }

   @Override
   public void validate() {
      if (compressionLevel < 0 || compressionLevel > 9) {
         throw logger.illegalCompressionLevel(compressionLevel);
      }
   }

   @Override
   public RestServerConfiguration create() {
      return new RestServerConfiguration(defaultCacheName, name, extendedHeaders, host, port, ignoredCaches, ssl.create(),
            startTransport, contextPath, adminOperationsHandler, maxContentLength, corsRules, compressionLevel);
   }

   @Override
   public Builder<?> read(RestServerConfiguration template) {
      this.extendedHeaders = template.extendedHeaders();
      this.host = template.host();
      this.port = template.port();
      this.maxContentLength = template.maxContentLength();
      this.corsRules = template.getCorsRules();
      this.compressionLevel = template.getCompressionLevel();
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
