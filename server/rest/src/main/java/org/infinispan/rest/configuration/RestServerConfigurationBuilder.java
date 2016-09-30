package org.infinispan.rest.configuration;

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

   private static final int DEFAULT_PORT = 8080;

   private ExtendedHeaders extendedHeaders = ExtendedHeaders.ON_DEMAND;

   public RestServerConfigurationBuilder() {
      super(DEFAULT_PORT);
   }

   public RestServerConfigurationBuilder extendedHeaders(ExtendedHeaders extendedHeaders) {
      this.extendedHeaders = extendedHeaders;
      return this;
   }

   @Override
   public void validate() {
      // Nothing to do
   }

   @Override
   public RestServerConfiguration create() {
      return new RestServerConfiguration(extendedHeaders, host, port, ignoredCaches, ssl.create());
   }

   @Override
   public Builder<?> read(RestServerConfiguration template) {
      this.extendedHeaders = template.extendedHeaders();
      this.host = template.host();
      this.port = template.port();
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
