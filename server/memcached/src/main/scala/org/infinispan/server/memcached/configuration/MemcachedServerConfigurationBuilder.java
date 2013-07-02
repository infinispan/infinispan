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
   private String cache = "memcachedCache";

   public MemcachedServerConfigurationBuilder() {
      super(11211);
   }

   @Override
   public MemcachedServerConfigurationBuilder self() {
      return this;
   }

   public MemcachedServerConfigurationBuilder cache(String cache) {
      this.cache = cache;
      return this;
   }

   @Override
   public MemcachedServerConfiguration create() {
      return new MemcachedServerConfiguration(cache, name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl.create(), tcpNoDelay, workerThreads);
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
      this.cache = template.cache();
      return this;
   }
}
