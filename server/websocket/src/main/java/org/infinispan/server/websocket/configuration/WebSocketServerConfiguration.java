package org.infinispan.server.websocket.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;

/**
 * WebSocketServerConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
@BuiltBy(WebSocketServerConfigurationBuilder.class)
public class WebSocketServerConfiguration extends ProtocolServerConfiguration {

   WebSocketServerConfiguration(String defaultCacheName, String name, String host, int port, int idleTimeout, int recvBufSize, int sendBufSize, SslConfiguration ssl, boolean tcpNoDelay, int workerThreads) {
      super(defaultCacheName, name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl, tcpNoDelay, workerThreads);
   }
}
