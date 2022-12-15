package org.infinispan.rest;

import java.util.Collections;

import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;

/**
 * Creates Netty Channels for this server.
 *
 * <p>
 * With ALPN support, this class acts only as a bridge between Server Core and ALPN Handler which bootstraps
 * pipeline handlers
 * </p>
 *
 * @author Sebastian Łaskawiec
 */
public class RestChannelInitializer extends NettyChannelInitializer<RestServerConfiguration> {

   static final int MAX_INITIAL_LINE_SIZE = 4096;
   static final int MAX_HEADER_SIZE = 8192;
   private final RestServer restServer;

   /**
    * Creates new {@link RestChannelInitializer}.
    *
    * @param restServer Rest Server this initializer belongs to.
    * @param transport  Netty transport.
    */
   public RestChannelInitializer(RestServer restServer, NettyTransport transport) {
      super(restServer, transport, null, null);
      this.restServer = restServer;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);
      if (server.getConfiguration().ssl().enabled()) {
         ch.pipeline().addLast(new ALPNHandler(restServer));
      } else {
         ALPNHandler.configurePipeline(ch.pipeline(), ApplicationProtocolNames.HTTP_1_1, restServer, Collections.emptyMap());
      }
   }

   @Override
   protected ApplicationProtocolConfig getAlpnConfiguration() {
      if (restServer.getConfiguration().ssl().enabled()) {
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

   public ChannelHandler getRestHandler() {
      return new RestRequestHandler(restServer);
   }

}
