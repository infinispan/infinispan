package org.infinispan.server.router.router.impl.singleport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.rest.ALPNHandler;
import org.infinispan.rest.RestServer;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.transport.NettyChannelInitializer;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.resp.RespServer;
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
      super(server, transport, null, null);
      this.restServer = restServer;
      this.upgradeServers = upgradeServers;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      super.initializeChannel(ch);
      upgradeServers.values().stream()
            .filter(ps -> ps instanceof HotRodServer).findFirst()
            .ifPresent(hotRodServer -> ch.pipeline().addLast(HotRodPingDetector.NAME, new HotRodPingDetector((HotRodServer) hotRodServer))
            );
      upgradeServers.values().stream()
            .filter(ps -> ps instanceof RespServer).findFirst()
            .ifPresent(respServer -> ch.pipeline().addLast(RespDetector.NAME, new RespDetector((RespServer) respServer))
            );
      if (server.getConfiguration().ssl().enabled()) {
         ch.pipeline().addLast(new ALPNHandler(restServer));
      } else {
         ALPNHandler.configurePipeline(ch.pipeline(), ApplicationProtocolNames.HTTP_1_1, restServer, upgradeServers);
      }
   }

   @Override
   protected ApplicationProtocolConfig getAlpnConfiguration() {
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
      }
      return null;
   }

}
