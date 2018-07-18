package org.infinispan.server.core.transport;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.logging.Log;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

/**
 * A Netty based transport.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 4.1
 */
public class NettyTransport implements Transport {

   static private final Log log = LogFactory.getLog(NettyTransport.class, Log.class);
   static private final boolean isLog4jAvailable;

   private static final String USE_EPOLL_PROPERTY = "infinispan.server.channel.epoll";
   private static final boolean IS_LINUX = System.getProperty("os.name").toLowerCase().startsWith("linux");
   private static final boolean EPOLL_DISABLED = System.getProperty(USE_EPOLL_PROPERTY, "true").equalsIgnoreCase("false");
   private static final boolean USE_NATIVE_EPOLL;

   static {
      boolean exception;
      try {
         Util.loadClassStrict("org.apache.logging.log4j.Logger", Thread.currentThread().getContextClassLoader());
         exception = false;
      } catch (ClassNotFoundException e) {
         exception = true;
      }
      isLog4jAvailable = !exception;

      if (Epoll.isAvailable()) {
         USE_NATIVE_EPOLL = !EPOLL_DISABLED && IS_LINUX;
      } else {
         if (IS_LINUX) {
            log.epollNotAvailable(Epoll.unavailabilityCause().toString());
         }
         USE_NATIVE_EPOLL = false;
      }
   }

   public NettyTransport(InetSocketAddress address, ProtocolServerConfiguration configuration, String threadNamePrefix,
                         EmbeddedCacheManager cacheManager) {
      this.address = address;
      this.configuration = configuration;

      // Need to initialize these in constructor since they require configuration
      masterGroup = buildEventLoop(1, new DefaultThreadFactory(threadNamePrefix + "-ServerMaster"));
      workerGroup = buildEventLoop(0, new DefaultThreadFactory(threadNamePrefix + "-ServerWorker"));

      serverChannels = new DefaultChannelGroup(threadNamePrefix + "-Channels", ImmediateEventExecutor.INSTANCE);
      acceptedChannels = new DefaultChannelGroup(threadNamePrefix + "-Accepted", ImmediateEventExecutor.INSTANCE);

      connectionStats = new NettyTransportConnectionStats(cacheManager, acceptedChannels, threadNamePrefix);
   }

   public void initializeHandler(ChannelInitializer<Channel> handler) {
      this.handler = handler;
   }

   private ChannelInitializer<Channel> handler;
   private final InetSocketAddress address;
   private final ProtocolServerConfiguration configuration;

   private final ChannelGroup serverChannels;
   final ChannelGroup acceptedChannels;

   private final EventLoopGroup masterGroup;
   private final EventLoopGroup workerGroup;

   private final NettyTransportConnectionStats connectionStats;

   private Optional<Integer> nettyPort = Optional.empty();

   @Override
   public void start() {
      // Make netty use log4j, otherwise it goes to JDK logging.
      if (isLog4jAvailable)
         InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);

      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap.group(masterGroup, workerGroup);
      bootstrap.channel(getServerSocketChannel());
      bootstrap.childHandler(handler);
      bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
      bootstrap.childOption(ChannelOption.TCP_NODELAY, configuration.tcpNoDelay()); // Sets server side tcpNoDelay
      if (configuration.sendBufSize() > 0)
         bootstrap.childOption(ChannelOption.SO_SNDBUF, configuration.sendBufSize()); // Sets server side send buffer
      if (configuration.recvBufSize() > 0)
         bootstrap.childOption(ChannelOption.SO_RCVBUF, configuration.recvBufSize()); // Sets server side receive buffer
      bootstrap.childOption(ChannelOption.SO_KEEPALIVE, configuration.tcpKeepAlive()); // Sets the keep-alive tcp flag

      Channel ch;
      try {
         ch = bootstrap.bind(address).sync().channel();
         nettyPort = Optional.of(((InetSocketAddress)ch.localAddress()).getPort());
      } catch (InterruptedException e) {
         throw new CacheException(e);
      }
      serverChannels.add(ch);
   }

   @Override
   public void stop() {
      Future<?> masterTerminationFuture = masterGroup.shutdownGracefully(100, 1000, TimeUnit.MILLISECONDS);
      Future<?> workerTerminationFuture = workerGroup.shutdownGracefully(100, 1000, TimeUnit.MILLISECONDS);

      masterTerminationFuture.awaitUninterruptibly();
      workerTerminationFuture.awaitUninterruptibly();

      // This is probably not necessary, all Netty resources should have been freed already
      ChannelGroupFuture serverChannelsTerminationFuture = serverChannels.close();
      ChannelGroupFuture acceptedChannelsTerminationFuture = acceptedChannels.close();

      ChannelGroupFuture future = serverChannelsTerminationFuture.awaitUninterruptibly();
      if (!future.isSuccess()) {
         log.serverDidNotUnbind();

         future.forEach(fut -> {
            Channel ch = fut.channel();
            if (ch.isActive()) {
               log.channelStillBound(ch, ch.remoteAddress());
            }
         });
      }

      future = acceptedChannelsTerminationFuture.awaitUninterruptibly();
      if (!future.isSuccess()) {
         log.serverDidNotClose();
         future.forEach(fut -> {
            Channel ch = fut.channel();
            if (ch.isActive()) {
               log.channelStillConnected(ch, ch.remoteAddress());
            }
         });
      }
      if (log.isDebugEnabled())
         log.debug("Channel group completely closed, external resources released");
      nettyPort = Optional.empty();
   }

   @Override
   public long getTotalBytesWritten() {
      return connectionStats.getTotalBytesWritten();
   }

   @Override
   public long getTotalBytesRead() {
      return connectionStats.getTotalBytesRead();
   }

   @Override
   public String getHostName() {
      return address.getHostName();
   }

   @Override
   public int getPort() {
      return nettyPort.orElse(address.getPort());
   }

   @Override
   public int getNumberWorkerThreads() {
      return configuration.workerThreads();
   }

   @Override
   public int getIdleTimeout() {
      return configuration.idleTimeout();
   }

   @Override
   public boolean getTcpNoDelay() {
      return configuration.tcpNoDelay();
   }

   @Override
   public int getSendBufferSize() {
      return configuration.sendBufSize();
   }

   @Override
   public int getReceiveBufferSize() {
      return configuration.recvBufSize();
   }

   @Override
   public int getNumberOfLocalConnections() {
      return connectionStats.getNumberOfLocalConnections();
   }

   @Override
   public int getNumberOfGlobalConnections() {
      return connectionStats.getNumberOfGlobalConnections();
   }

   public void updateTotalBytesWritten(int bytes) {
      connectionStats.incrementTotalBytesWritten(bytes);
   }

   public void updateTotalBytesRead(int bytes) {
      connectionStats.incrementTotalBytesRead(bytes);
   }

   private Class<? extends ServerChannel> getServerSocketChannel() {
      Class<? extends ServerChannel> channel = USE_NATIVE_EPOLL ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
      log.createdSocketChannel(channel.getName(), configuration.toString());
      return channel;
   }

   private EventLoopGroup buildEventLoop(int nThreads, DefaultThreadFactory threadFactory) {
      EventLoopGroup eventLoop = USE_NATIVE_EPOLL ? new EpollEventLoopGroup(nThreads, threadFactory) :
              new NioEventLoopGroup(nThreads, threadFactory);
      log.createdNettyEventLoop(eventLoop.getClass().getName(), configuration.toString());
      return eventLoop;
   }
}
