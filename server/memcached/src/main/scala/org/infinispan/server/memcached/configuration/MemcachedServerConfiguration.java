package org.infinispan.server.memcached.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

/**
 * MemcachedServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@BuiltBy(MemcachedServerConfigurationBuilder.class)
public class MemcachedServerConfiguration extends ProtocolServerConfiguration {
   private final String cache;

   MemcachedServerConfiguration(String cache, String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads) {
      super(name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads);
      this.cache = cache;
   }

   public String cache() {
      return cache;
   }

   @Override
   public String toString() {
      return "MemcachedServerConfiguration [cache=" + cache + ", " + super.toString() + "]";
   }
}
