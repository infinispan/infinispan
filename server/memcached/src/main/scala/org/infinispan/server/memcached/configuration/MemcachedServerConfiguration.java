package org.infinispan.server.memcached.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

import java.util.Set;

/**
 * MemcachedServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@BuiltBy(MemcachedServerConfigurationBuilder.class)
public class MemcachedServerConfiguration extends ProtocolServerConfiguration {

   MemcachedServerConfiguration(String defaultCacheName, String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads, Set<String> ignoredCaches) {
      super(defaultCacheName, name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads, ignoredCaches);
   }

   @Override
   public String toString() {
      return "MemcachedServerConfiguration [" + super.toString() + "]";
   }
}
