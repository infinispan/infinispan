package org.infinispan.rest.configuration;

import static org.infinispan.rest.configuration.RestServerConfiguration.COMPRESSION_LEVEL;
import static org.infinispan.rest.configuration.RestServerConfiguration.COMPRESSION_THRESHOLD;
import static org.infinispan.rest.configuration.RestServerConfiguration.CONTEXT_PATH;
import static org.infinispan.rest.configuration.RestServerConfiguration.EXTENDED_HEADERS;
import static org.infinispan.rest.configuration.RestServerConfiguration.MAX_CONTENT_LENGTH;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.NAME;

import java.nio.file.Path;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
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
public class RestServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<RestServerConfiguration, RestServerConfigurationBuilder, RestAuthenticationConfiguration> implements
      Builder<RestServerConfiguration> {

   static final Log logger = LogFactory.getLog(RestServerConfigurationBuilder.class, Log.class);

   private final RestAuthenticationConfigurationBuilder authentication;
   private final CorsConfigurationBuilder cors;
   private Path staticResources;
   private final EncryptionConfigurationBuilder encryption = new EncryptionConfigurationBuilder(ssl());

   private static final int DEFAULT_PORT = 8080;
   private static final String DEFAULT_NAME = "rest";

   public RestServerConfigurationBuilder() {
      super(DEFAULT_PORT, RestServerConfiguration.attributeDefinitionSet());
      this.authentication = new RestAuthenticationConfigurationBuilder(this);
      this.cors = new CorsConfigurationBuilder();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public RestServerConfigurationBuilder extendedHeaders(ExtendedHeaders extendedHeaders) {
      attributes.attribute(EXTENDED_HEADERS).set(extendedHeaders);
      return this;
   }

   public RestServerConfigurationBuilder contextPath(String contextPath) {
      attributes.attribute(CONTEXT_PATH).set(contextPath);
      return this;
   }

   public RestServerConfigurationBuilder maxContentLength(String maxContentLength) {
      attributes.attribute(MAX_CONTENT_LENGTH).set(maxContentLength);
      return this;
   }

   public RestServerConfigurationBuilder compressionLevel(int compressLevel) {
      attributes.attribute(COMPRESSION_LEVEL).set(compressLevel);
      return this;
   }

   public RestServerConfigurationBuilder compressionThreshold(int compressThreshold) {
      attributes.attribute(COMPRESSION_THRESHOLD).set(compressThreshold);
      return this;
   }

   @Override
   public RestAuthenticationConfigurationBuilder authentication() {
      return authentication;
   }

   public EncryptionConfigurationBuilder encryption() {
      return encryption;
   }

   public RestServerConfigurationBuilder addAll(List<CorsConfig> corsConfig) {
      cors.add(corsConfig);
      return this;
   }

   public RestServerConfigurationBuilder staticResources(Path dir) {
      this.staticResources = dir;
      return this;
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
      if (!attributes.attribute(NAME).isModified()) {
         String socketBinding = socketBinding();
         name(DEFAULT_NAME + (socketBinding == null ? "" : "-" + socketBinding));
      }
      return new RestServerConfiguration(attributes.protect(), ssl.create(), staticResources, authentication.create(), cors.create(), encryption.create(), ipFilter.create());
   }

   @Override
   public Builder<?> read(RestServerConfiguration template, Combine combine) {
      super.read(template, combine);
      this.attributes.read(template.attributes(), combine);
      this.authentication.read(template.authentication(), combine);
      this.cors.read(template.cors(), combine);
      this.encryption.read(template.encryption(), combine);
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
