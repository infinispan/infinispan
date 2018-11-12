package org.infinispan.server.router.router.impl.singleport;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.rest.RestServer;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.transport.NettyInitializers;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.router.RoutingTable;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.router.EndpointRouter;
import org.infinispan.server.router.routes.hotrod.HotRodServerRouteDestination;
import org.infinispan.server.router.routes.rest.RestServerRouteDestination;
import org.infinispan.server.router.routes.singleport.SinglePortRouteSource;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;

public class SinglePortEndpointRouter extends AbstractProtocolServer<SinglePortRouterConfiguration> implements EndpointRouter {

   private static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

   private RoutingTable routingTable;

   public SinglePortEndpointRouter(SinglePortRouterConfiguration configuration) {
      super(Protocol.SINGLE_PORT.toString());
      this.configuration = configuration;
   }

   @Override
   public void start(RoutingTable routingTable) {
      this.routingTable = routingTable;
      InetSocketAddress address = new InetSocketAddress(configuration.host(), configuration.port());
      transport = new NettyTransport(address, configuration, getQualifiedName(), cacheManager);
      transport.initializeHandler(getInitializer());
      transport.start();
      logger.restRouterStarted(getTransport().getHostName() + ":" + getTransport().getPort());
   }

   @Override
   public void stop() {
     super.stop();
   }

   @Override
   public InetAddress getIp() {
      try {
         return InetAddress.getByName(getHost());
      } catch (UnknownHostException e) {
         throw new IllegalStateException("Unknown host", e);
      }
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   @Override
   public ChannelInboundHandler getDecoder() {
      return null;
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      Map<String, ProtocolServer> upgradeServers = new HashMap<>();

      RestServer restServer = routingTable.streamRoutes(SinglePortRouteSource.class, RestServerRouteDestination.class)
            .findFirst()
            .map(r -> r.getRouteDesitnation().getRestServer())
            .orElseThrow(() -> new IllegalStateException("There must be a REST route!"));

      routingTable.streamRoutes(SinglePortRouteSource.class, HotRodServerRouteDestination.class)
            .findFirst()
            .ifPresent(r -> upgradeServers.put("HR", r.getRouteDesitnation().getHotrodServer()));

      SinglePortChannelInitializer restChannelInitializer = new SinglePortChannelInitializer(this, transport, restServer, upgradeServers);
      return new NettyInitializers(restChannelInitializer);
   }

   @Override
   public Protocol getProtocol() {
      return Protocol.SINGLE_PORT;
   }

   @Override
   public int getWorkerThreads() {
      return 1;
   }

}
