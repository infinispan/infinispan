package org.infinispan.server.router.router.impl.rest;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.infinispan.server.router.RoutingTable;
import org.infinispan.server.router.configuration.RestRouterConfiguration;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.router.EndpointRouter;
import org.infinispan.server.router.router.impl.rest.handlers.ChannelInboundHandlerDelegatorInitializer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;

public class RestEndpointRouter implements EndpointRouter {
   private static final String THREAD_NAME_PREFIX = "EndpointRouter";

   private final NioEventLoopGroup masterGroup = new NioEventLoopGroup(1, new DefaultThreadFactory(THREAD_NAME_PREFIX + "-ServerMaster"));
   private final NioEventLoopGroup workerGroup = new NioEventLoopGroup(0, new DefaultThreadFactory(THREAD_NAME_PREFIX + "-ServerWorker"));
   private final RestRouterConfiguration configuration;
   private Integer port = null;
   private InetAddress ip = null;

   public RestEndpointRouter(RestRouterConfiguration configuration) {
      this.configuration = configuration;
   }

   @Override
   public void start(RoutingTable routingTable) {
      try {
         ServerBootstrap bootstrap = new ServerBootstrap();
         bootstrap.group(masterGroup, workerGroup)
               .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
               .childHandler(new ChannelInboundHandlerDelegatorInitializer(routingTable))
               .channel(NioServerSocketChannel.class);

         InetAddress ip = configuration.getIp();
         int port = configuration.getPort();

         Channel channel = bootstrap.bind(ip, port).sync().channel();
         InetSocketAddress localAddress = (InetSocketAddress) channel.localAddress();
         this.port = localAddress.getPort();
         this.ip = localAddress.getAddress();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } catch (Exception e) {
         throw RouterLogger.SERVER.restRouterStartFailed(e);
      }

      RouterLogger.SERVER.debugf("REST EndpointRouter listening on %s:%d", ip, port);
   }

   @Override
   public void stop() {
      Future<?> masterGroupShutdown = masterGroup.shutdownGracefully();
      Future<?> workerGroupShutdown = workerGroup.shutdownGracefully();
      try {
         masterGroupShutdown.get();
         workerGroupShutdown.get();
      } catch (Exception e) {
         RouterLogger.SERVER.errorWhileShuttingDown(e);
      }
      port = null;
      ip = null;
   }

   @Override
   public String getHost() {
      return ip.getHostAddress();
   }

   @Override
   public InetAddress getIp() {
      return ip;
   }

   @Override
   public Integer getPort() {
      return port;
   }

   @Override
   public Protocol getProtocol() {
      return Protocol.REST;
   }
}
