package org.infinispan.server.router.router.impl.singleport;

import java.util.Map;

import org.infinispan.rest.RestServer;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.channel.Channel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;

/**
 * Netty pipeline initializer for Single Port
 *
 * @author Sebastian Łaskawiec
 */
class SinglePortChannelInitializer extends NettyChannelInitializer {

   private final SinglePortUpgradeHandler http11To2UpgradeHandler;

   public SinglePortChannelInitializer(SinglePortEndpointRouter server, NettyTransport transport, RestServer restServer, Map<String, ProtocolServer> upgradeServers) {
      super(server, transport, null, null);
      http11To2UpgradeHandler = new SinglePortUpgradeHandler(server.getConfiguration().ssl().enabled(), restServer, upgradeServers);
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

}
