package org.infinispan.server.router.router.impl.rest;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.logging.LogFactory;
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

   private static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

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
         throw logger.restRouterStartFailed(e);
      }

      logger.restRouterStarted(ip + ":" + port);
   }

   @Override
   public void stop() {
      CompletableFuture<?> masterGroupShutdown = wrapShutdownFuture(masterGroup.shutdownGracefully());
      CompletableFuture<?> workerGroupShutdown = wrapShutdownFuture(workerGroup.shutdownGracefully());
      try {
         CompletableFuture.allOf(masterGroupShutdown, workerGroupShutdown).get();
      } catch (Exception e) {
         logger.errorWhileShuttingDown(e);
      }
      port = null;
      ip = null;
   }

   @Override
   public InetAddress getIp() {
      return ip;
   }

   @Override
   public Integer getPort() {
      return port;
   }



   private <U> CompletableFuture<U> wrapShutdownFuture(Future<U> shutdownFuture) {
      return CompletableFuture.supplyAsync(() -> {
         try {
            return shutdownFuture.get();
         } catch (Exception e) {
            throw new IllegalStateException(e);
         }
      });
   }

   @Override
   public Protocol getProtocol() {
      return Protocol.REST;
   }
}
