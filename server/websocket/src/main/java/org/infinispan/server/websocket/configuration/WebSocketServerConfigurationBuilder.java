package org.infinispan.server.websocket.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * WebSocketServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class WebSocketServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<WebSocketServerConfiguration, WebSocketServerConfigurationBuilder> implements
      Builder<WebSocketServerConfiguration> {

   public WebSocketServerConfigurationBuilder() {
      super(8181);
   }

   @Override
   public WebSocketServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public WebSocketServerConfiguration create() {
      return new WebSocketServerConfiguration(defaultCacheName, name, host, port, idleTimeout, recvBufSize, sendBufSize, ssl.create(), tcpNoDelay, workerThreads);
   }

   public WebSocketServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public WebSocketServerConfiguration build() {
      return build(true);
   }

}
