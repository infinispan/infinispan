package org.infinispan.rest.configuration;

import static org.infinispan.rest.configuration.RestServerConfiguration.COMPRESSION_LEVEL;
import static org.infinispan.rest.configuration.RestServerConfiguration.CONTEXT_PATH;
import static org.infinispan.rest.configuration.RestServerConfiguration.EXTENDED_HEADERS;
import static org.infinispan.rest.configuration.RestServerConfiguration.MAX_CONTENT_LENGTH;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
   public static final int CROSS_ORIGIN_PORT = 11222;
   public static final int CROSS_ORIGIN_ALT_PORT = 9000;

   private final AuthenticationConfigurationBuilder authentication;
   private final CorsConfigurationBuilder cors;
   private Path staticResources;
   private final EncryptionConfigurationBuilder encryption = new EncryptionConfigurationBuilder(ssl());

   public static final int DEFAULT_PORT = 8080;
   public static final String DEFAULT_NAME = "rest";
   public static final String SERVER_HOME = "infinispan.server.home.path";
   public static final String STATIC_RESOURCES_PATH = "static";

   public RestServerConfigurationBuilder() {
      super(DEFAULT_PORT, RestServerConfiguration.attributeDefinitionSet());
      String serverHome = System.getProperty(SERVER_HOME);
      if (serverHome != null) staticResources = Paths.get(serverHome, STATIC_RESOURCES_PATH);
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

   public RestServerConfigurationBuilder corsAllowForLocalhost(Set<String> schemes, int... ports) {
      cors.corsAllowForLocalhost(schemes, ports);
      return this;
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
      Set<String> schemes = new HashSet<>();
      schemes.add("http");
      schemes.add("https");
      corsAllowForLocalhost(schemes, DEFAULT_PORT, CROSS_ORIGIN_PORT, CROSS_ORIGIN_ALT_PORT);
      return new RestServerConfiguration(attributes.protect(), ssl.create(), staticResources, authentication.create(), cors.create(), encryption.create());
   }

   @Override
   public Builder<?> read(RestServerConfiguration template) {
      super.read(template);
      this.attributes.read(template.attributes());
      this.authentication.read(template.authentication());
      this.cors.read(template.cors());
      this.encryption.read(template.encryption());
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
