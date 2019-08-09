package org.infinispan.rest.configuration;

import static org.infinispan.rest.configuration.RestServerConfiguration.COMPRESSION_LEVEL;
import static org.infinispan.rest.configuration.RestServerConfiguration.CONTEXT_PATH;
import static org.infinispan.rest.configuration.RestServerConfiguration.EXTENDED_HEADERS;
import static org.infinispan.rest.configuration.RestServerConfiguration.MAX_CONTENT_LENGTH;

import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.rest.logging.Log;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.http.cors.CorsConfig;

/**
 * RestServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class RestServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<RestServerConfiguration, RestServerConfigurationBuilder> implements
      Builder<RestServerConfiguration> {

   final static Log logger = LogFactory.getLog(RestServerConfigurationBuilder.class, Log.class);

   private final AuthenticationConfigurationBuilder authentication;
   private final CorsConfigurationBuilder cors;
   private final EncryptionConfigurationBuilder encryption = new EncryptionConfigurationBuilder(ssl());

   public static final String DEFAULT_CONTEXT_PATH = "rest";
   public static final int DEFAULT_PORT = 8080;
   public static final String DEFAULT_NAME = "rest";

   public RestServerConfigurationBuilder() {
      super(DEFAULT_PORT, RestServerConfiguration.attributeDefinitionSet());
      name(DEFAULT_NAME);
      this.authentication = new AuthenticationConfigurationBuilder(this);
      this.cors = new CorsConfigurationBuilder();
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
      cors.corsAllowForLocalhost(scheme, port);
      return this;
   }

   public EncryptionConfigurationBuilder encryption() {
      return encryption;
   }

   public RestServerConfigurationBuilder addAll(List<CorsConfig> corsConfig) {
      cors.add(corsConfig);
      return this;
   }

   public AuthenticationConfigurationBuilder authentication() {
      return authentication;
   }

   public CorsConfigurationBuilder cors() {
      return cors;
   }

   @Override
   public void validate() {
      super.validate();
      authentication.validate();
      int compressionLevel = attributes.attribute(COMPRESSION_LEVEL).get();
      if (compressionLevel < 0 || compressionLevel > 9) {
         throw logger.illegalCompressionLevel(compressionLevel);
      }
   }

   @Override
   public RestServerConfiguration create() {
      return new RestServerConfiguration(attributes.protect(), ssl.create(), authentication.create(), cors.create(), encryption.create());
   }

   @Override
   public Builder<?> read(RestServerConfiguration template) {
      super.read(template);
      //TODO:[gustavo] read other attributes
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

}
