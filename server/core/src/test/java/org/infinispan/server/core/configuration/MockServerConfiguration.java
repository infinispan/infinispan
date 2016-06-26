package org.infinispan.server.core.configuration;

import java.util.Set;

public class MockServerConfiguration extends ProtocolServerConfiguration {

   protected MockServerConfiguration(String defaultCacheName, String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads, Set<String> ignoredCaches) {
      super(defaultCacheName, name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads, ignoredCaches);
   }
}
