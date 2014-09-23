package org.infinispan.server.memcached.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * MemcachedServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class MemcachedServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<MemcachedServerConfiguration, MemcachedServerConfigurationBuilder> implements
      Builder<MemcachedServerConfiguration> {

   public MemcachedServerConfigurationBuilder() {
      super(11211);
      this.defaultCacheName("memcachedCache");
   }

   @Override
   public MemcachedServerConfigurationBuilder self() {
      return this;
   }

   /**
    * Use {@link ProtocolServerConfigurationBuilder#defaultCacheName(String)} instead
    */
   @Deprecated
   public MemcachedServerConfigurationBuilder cache(String cache) {
      this.defaultCacheName(cache);
      return this;
   }

   @Override
   public MemcachedServerConfiguration create() {
      return new MemcachedServerConfiguration(defaultCacheName, name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl.create(), tcpNoDelay, workerThreads);
   }

   public MemcachedServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public MemcachedServerConfiguration build() {
      return build(true);
   }

   @Override
   public Builder<?> read(MemcachedServerConfiguration template) {
      super.read(template);
      return this;
   }
}
