package org.infinispan.server.memcached.configuration;

import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.admin.AdminOperationsHandler;
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

   public static final int DEFAULT_MEMCACHED_PORT = 11211;
   public static final String DEFAULT_MEMCACHED_CACHE = "memcachedCache";

   MemcachedServerConfiguration(String defaultCacheName, String name, String host, int port,
                                int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl,
                                boolean tcpNoDelay, boolean tcpKeepAlive, int workerThreads,
                                Set<String> ignoredCaches, boolean startTransport,
                                AdminOperationsHandler adminOperationsHandler) {
      super(defaultCacheName, name, host, port,
            idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, tcpKeepAlive, workerThreads,
            ignoredCaches, startTransport, adminOperationsHandler);
   }

   @Override
   public String toString() {
      return "MemcachedServerConfiguration [" + super.toString() + "]";
   }
}
