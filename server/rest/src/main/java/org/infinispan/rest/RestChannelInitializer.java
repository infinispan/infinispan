package org.infinispan.rest;

import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.channel.Channel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;

/**
 * Creates Netty Channels for this server.
 *
 * <p>
 *    With ALPN support, this class acts only as a bridge between Server Core and ALPN Handler which bootstraps
 *    pipeline handlers
 * </p>
 *
 * @author Sebastian Łaskawiec
 */
public class RestChannelInitializer extends NettyChannelInitializer {

   static final int MAX_INITIAL_LINE_SIZE = 4096;
   static final int MAX_HEADER_SIZE = 8192;

   private final ALPNHandler alpnHandler;

   private final RestServer restServer;

   /**
    * Creates new {@link RestChannelInitializer}.
    *
    * @param restServer Rest Server this initializer belongs to.
    * @param transport Netty transport.
    */
   public RestChannelInitializer(RestServer restServer, NettyTransport transport) {
      super(restServer, transport, null, null);
      this.restServer = restServer;
      alpnHandler = new ALPNHandler(restServer);
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);
      if (server.getConfiguration().ssl().enabled()) {
         configureSSL(ch);
      } else {
         alpnHandler.configurePipeline(ch.pipeline(), ApplicationProtocolNames.HTTP_1_1);
      }
   }

   private void configureSSL(Channel ch) {
      ch.pipeline().addLast(alpnHandler);
   }

   protected int maxContentLength() {
      return this.restServer.getConfiguration().maxContentLength() + MAX_INITIAL_LINE_SIZE + MAX_HEADER_SIZE;
   }

   @Override
   protected ApplicationProtocolConfig getAlpnConfiguration() {
      return alpnHandler.getAlpnConfiguration();
   }

   public ALPNHandler getAlpnHandler() {
      return alpnHandler;
   }
}
