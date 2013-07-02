package org.infinispan.server.core.configuration;

public class MockServerConfiguration extends ProtocolServerConfiguration {

   protected MockServerConfiguration(String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads) {
      super(name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads);
   }
}
