package org.infinispan.server.core.configuration;

import java.util.Set;

import org.infinispan.server.core.admin.AdminOperationsHandler;

public class MockServerConfiguration extends ProtocolServerConfiguration {

   protected MockServerConfiguration(String defaultCacheName, String name, String host, int port, int idleTimeout,
                                     int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay,
                                     boolean tcpKeepAlive, int workerThreads,
                                     Set<String> ignoredCaches, Boolean startTransport,
                                     AdminOperationsHandler adminOperationsHandler) {
      super(defaultCacheName, name, host, port,
            idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, tcpKeepAlive, workerThreads,
            ignoredCaches, startTransport, adminOperationsHandler);
   }
}
