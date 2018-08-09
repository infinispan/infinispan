package org.infinispan.rest.configuration;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;

import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.rest.logging.Log;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;

import static org.infinispan.rest.configuration.RestServerConfiguration.COMPRESSION_LEVEL;
import static org.infinispan.rest.configuration.RestServerConfiguration.CONTEXT_PATH;
import static org.infinispan.rest.configuration.RestServerConfiguration.CORS_RULES;
import static org.infinispan.rest.configuration.RestServerConfiguration.EXTENDED_HEADERS;
import static org.infinispan.rest.configuration.RestServerConfiguration.MAX_CONTENT_LENGTH;

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

   public RestServerConfigurationBuilder() {
      super(DEFAULT_PORT, RestServerConfiguration.attributeDefinitionSet());
      name(DEFAULT_NAME);
   }

   public RestServerConfigurationBuilder extendedHeaders(ExtendedHeaders extendedHeaders) {
      attributes.attribute(EXTENDED_HEADERS).set(extendedHeaders);
      return this;
   }

   public RestServerConfigurationBuilder contextPath(String contextPath) {
      attributes.attribute(CONTEXT_PATH).set(contextPath);
      return this;
   }

   public RestServerConfigurationBuilder maxContentLength(int maxContentLength) {
      attributes.attribute(MAX_CONTENT_LENGTH).set(maxContentLength);
      return this;
   }

   public RestServerConfigurationBuilder compressionLevel(int compressLevel) {
      attributes.attribute(COMPRESSION_LEVEL).set(compressLevel);
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
      attributes.attribute(CORS_RULES).get().add(corsConfig);
      return this;
   }

   public RestServerConfigurationBuilder addAll(List<CorsConfig> corsConfig) {
      attributes.attribute(CORS_RULES).get().addAll(corsConfig);
      return this;
   }

   @Override
   public void validate() {
      int compressionLevel = attributes.attribute(COMPRESSION_LEVEL).get();
      if (compressionLevel < 0 || compressionLevel > 9) {
         throw logger.illegalCompressionLevel(compressionLevel);
      }
   }

   @Override
   public RestServerConfiguration create() {
      return new RestServerConfiguration(attributes.protect(), ssl.create());
   }

   @Override
   public Builder<?> read(RestServerConfiguration template) {
      super.read(template);
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
   public RestServerConfigurationBuilder tcpKeepAlive(boolean tcpKeepAlive) {
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
