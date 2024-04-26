package org.infinispan.server.router.router.impl.singleport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.rest.ALPNHandler;
import org.infinispan.rest.RestServer;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;

import io.netty.channel.Channel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;

/**
 * Netty pipeline initializer for Single Port
 *
 * @author Sebastian Łaskawiec
 */
class SinglePortChannelInitializer extends NettyChannelInitializer<SinglePortRouterConfiguration> {

   private final RestServer restServer;
   private final Map<String, ProtocolServer<?>> upgradeServers;

   public SinglePortChannelInitializer(SinglePortEndpointRouter server, NettyTransport transport, RestServer restServer, Map<String, ProtocolServer<?>> upgradeServers) {
      super(server, transport, null, null, getAlpnConfiguration(server, upgradeServers));
      this.restServer = restServer;
      this.upgradeServers = upgradeServers;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);
      for(ProtocolServer<?> ps : upgradeServers.values()) {
         ps.installDetector(ch);
      }
      if (server.getConfiguration().ssl().enabled()) {
         ch.pipeline().addLast(new ALPNHandler(restServer));
      } else {
         ALPNHandler.configurePipeline(ch.pipeline(), ApplicationProtocolNames.HTTP_1_1, restServer, upgradeServers);
      }
   }

   private static ApplicationProtocolConfig getAlpnConfiguration(SinglePortEndpointRouter server, Map<String, ProtocolServer<?>> upgradeServers) {
      if (server.getConfiguration().ssl().enabled()) {
         List<String> supportedProtocols = new ArrayList<>();
         supportedProtocols.add(ApplicationProtocolNames.HTTP_2);
         supportedProtocols.add(ApplicationProtocolNames.HTTP_1_1);
         supportedProtocols.addAll(upgradeServers.keySet());

         return new ApplicationProtocolConfig(
               ApplicationProtocolConfig.Protocol.ALPN,
               // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
               ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
               // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
               ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
               supportedProtocols);
      } else {
         return null;
      }
   }

}
