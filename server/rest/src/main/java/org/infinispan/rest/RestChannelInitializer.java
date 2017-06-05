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

   private final AlpnHandler alpnHandler;

   /**
    * Creates new {@link RestChannelInitializer}.
    *
    * @param server Rest Server this initializer belongs to.
    * @param transport Netty transport.
    */
   public RestChannelInitializer(RestServer server, NettyTransport transport) {
      super(server, transport, null, null);
      alpnHandler = new AlpnHandler(server);
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);
      if (server.getConfiguration().ssl().enabled()) {
         ch.pipeline().addLast(alpnHandler);
      } else {
         alpnHandler.configurePipeline(ch.pipeline(), ApplicationProtocolNames.HTTP_1_1);
      }
   }

   @Override
   protected ApplicationProtocolConfig getAlpnConfiguration() {
      if (server.getConfiguration().ssl().enabled()) {
         return new ApplicationProtocolConfig(
               ApplicationProtocolConfig.Protocol.ALPN,
               // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
               ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
               // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
               ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
               ApplicationProtocolNames.HTTP_2,
               ApplicationProtocolNames.HTTP_1_1);
      }
      return null;
   }

   public AlpnHandler getAlpnHandler() {
      return alpnHandler;
   }
}
