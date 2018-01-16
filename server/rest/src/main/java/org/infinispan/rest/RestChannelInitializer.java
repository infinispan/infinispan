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

   private final Http11To2UpgradeHandler http11To2UpgradeHandler;

   /**
    * Creates new {@link RestChannelInitializer}.
    *
    * @param server Rest Server this initializer belongs to.
    * @param transport Netty transport.
    */
   public RestChannelInitializer(RestServer server, NettyTransport transport) {
      super(server, transport, null, null);
      http11To2UpgradeHandler = new Http11To2UpgradeHandler(server);
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);
      if (server.getConfiguration().ssl().enabled()) {
         ch.pipeline().addLast(http11To2UpgradeHandler);
      } else {
         http11To2UpgradeHandler.configurePipeline(ch.pipeline(), ApplicationProtocolNames.HTTP_1_1);
      }
   }

   @Override
   protected ApplicationProtocolConfig getAlpnConfiguration() {
      return http11To2UpgradeHandler.getAlpnConfiguration();
   }

   public Http11To2UpgradeHandler getHttp11To2UpgradeHandler() {
      return http11To2UpgradeHandler;
   }
}
