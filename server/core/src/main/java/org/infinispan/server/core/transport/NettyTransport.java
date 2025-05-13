package org.infinispan.server.core.transport;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.logging.Log;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFutureListener;
import io.netty.channel.group.ChannelMatcher;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import net.jcip.annotations.GuardedBy;

/**
 * A Netty based transport.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 4.1
 */
@MBean(objectName = "Transport",
      description = "Transport component manages read and write operations to/from server.")
@Scope(Scopes.SERVER)
public class NettyTransport implements Transport {

   private static final Log log = LogFactory.getLog(NettyTransport.class, Log.class);
   private static final boolean isLog4jAvailable = isIsLog4jAvailable();
   private final DefaultThreadFactory masterThreadFactory;
   private final DefaultThreadFactory ioThreadFactory;

   private ChannelInitializer<Channel> handler;
   private final InetSocketAddress address;
   private final ProtocolServerConfiguration configuration;

   private final ChannelGroup serverChannels;
   private final EmbeddedCacheManager cacheManager;
   final ChannelGroup acceptedChannels;

   private EventLoopGroup masterGroup;
   private EventLoopGroup ioGroup;

   private final NettyTransportConnectionStats connectionStats;

   //-1 if not set
   private int nettyPort = -1;

   // This method is here to be replaced by Quarkus
   private static boolean isIsLog4jAvailable() {
      try {
         Util.loadClassStrict("org.apache.logging.log4j.Logger", Thread.currentThread().getContextClassLoader());
         return true;
      } catch (ClassNotFoundException e) {
         return false;
      }
   }

   private boolean running;

   public NettyTransport(InetSocketAddress address, ProtocolServerConfiguration configuration, String threadNamePrefix,
                         EmbeddedCacheManager cacheManager) {
      this.address = address;
      this.configuration = configuration;

      masterThreadFactory = new DefaultThreadFactory(threadNamePrefix + "-ServerMaster");
      ioThreadFactory = new DefaultThreadFactory(threadNamePrefix + "-ServerIO");

      serverChannels = new DefaultChannelGroup(threadNamePrefix + "-Channels", ImmediateEventExecutor.INSTANCE);
      acceptedChannels = new DefaultChannelGroup(threadNamePrefix + "-Accepted", ImmediateEventExecutor.INSTANCE);

      this.cacheManager = cacheManager;
      connectionStats = new NettyTransportConnectionStats(cacheManager, acceptedChannels, threadNamePrefix);
   }

   public void initializeHandler(ChannelInitializer<Channel> handler) {
      this.handler = handler;
   }

   @ManagedOperation(
         description = "Starts the transport",
         displayName = "Starts the transport",
         name = "start"
   )
   @Override
   public synchronized void start() {
      if (running) {
         return;
      }
      // Make netty use log4j, otherwise it goes to JDK logging.
      if (isLog4jAvailable)
         InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);

      // Need to initialize these in constructor since they require configuration
      masterGroup = buildEventLoop(1, masterThreadFactory, configuration.toString());
      // Need to initialize these in constructor since they require configuration. probably we need to inject the ioGroup in the constructor somehow.
      if (cacheManager == null) { //it is null for single-port endpoint. probably we need to inject the ioGroup in the constructor.
         ioGroup = buildEventLoop(configuration.ioThreads(), ioThreadFactory, configuration.toString());
      } else {
         ioGroup = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(EventLoopGroup.class);
      }

      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap.group(masterGroup, ioGroup);
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
         nettyPort =((InetSocketAddress) ch.localAddress()).getPort();
      } catch (InterruptedException e) {
         stopInternal();
         throw new CacheException(e);
      } catch (Throwable t) {
         stopInternal();
         throw t;
      }
      serverChannels.add(ch);
      running = true;
   }

   @ManagedOperation(
         description = "Stops the transport",
         displayName = "Stops the transport",
         name = "stop"
   )
   @Override
   public synchronized void stop() {
      if (running) {
         stopInternal();
      }
   }

   @GuardedBy("this")
   private void stopInternal() {
      Future<?> masterTerminationFuture = masterGroup.shutdownGracefully(100, 1000, TimeUnit.MILLISECONDS);
      if (cacheManager == null) {
         Future<?> ioTerminationFuture = ioGroup.shutdownGracefully(100, 1000, TimeUnit.MILLISECONDS);
         ioTerminationFuture.awaitUninterruptibly();
      }

      masterTerminationFuture.awaitUninterruptibly();

      if (serverChannels.isEmpty() && acceptedChannels.isEmpty()) {
         log.debug("Channel group completely closed, external resources released");
      } else {
         serverChannels.forEach(ch -> {
            if (ch.isActive()) {
               log.channelStillBound(ch, ch.remoteAddress());
               ch.close().awaitUninterruptibly();
            }
         });
         acceptedChannels.forEach(ch -> {
            if (ch.isActive()) {
               log.channelStillConnected(ch, ch.remoteAddress());
               ch.close().awaitUninterruptibly();
            }
         });
      }
      nettyPort = -1;
      running = false;
   }

   @ManagedAttribute(
         description = "Returns whether the transport is running",
         displayName = "Transport running",
         dataType = DataType.TRAIT
   )
   @Override
   public synchronized boolean isRunning() {
      return running;
   }

   @ManagedAttribute(
         description = "Returns the total number of bytes written " +
               "by the server back to clients which includes both protocol and user information.",
         displayName = "Number of total number of bytes written",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getTotalBytesWritten() {
      return connectionStats.getTotalBytesWritten();
   }

   @ManagedAttribute(description = "Returns the total number of bytes read " +
         "by the server from clients which includes both protocol and user information.",
         displayName = "Number of total number of bytes read",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getTotalBytesRead() {
      return connectionStats.getTotalBytesRead();
   }

   @ManagedAttribute(
         description = "Returns the host to which the transport binds.",
         displayName = "Host name",
         dataType = DataType.TRAIT
   )
   @Override
   public String getHostName() {
      return address.getHostName();
   }

   @ManagedAttribute(
         description = "Returns the port to which the transport binds.",
         displayName = "Port",
         dataType = DataType.TRAIT
   )
   @Override
   public int getPort() {
      return nettyPort == -1 ? address.getPort() : nettyPort;
   }

   @ManagedAttribute(
         description = "Returns the number of I/O threads.",
         displayName = "Number of I/O threads"
   )
   @Override
   public int getNumberIOThreads() {
      int count = 0;
      for (EventExecutor unused : ioGroup) {
         count++;
      }
      return count;
   }

   @ManagedAttribute(
         description = "Returns the number of pending tasks.",
         displayName = "Pending tasks"
   )
   @Override
   public int getPendingTasks() {
      AtomicInteger count = new AtomicInteger(0);
      ioGroup.forEach(ee -> count.addAndGet(((SingleThreadEventExecutor) ee).pendingTasks()));
      return count.get();
   }

   @ManagedAttribute(
         description = "Returns the idle timeout.",
         displayName = "Idle timeout",
         dataType = DataType.TRAIT
   )
   @Override
   public int getIdleTimeout() {
      return configuration.idleTimeout();
   }

   @ManagedAttribute(
         description = "Returns whether TCP no delay was configured or not.",
         displayName = "TCP no delay",
         dataType = DataType.TRAIT
   )
   @Override
   public boolean getTcpNoDelay() {
      return configuration.tcpNoDelay();
   }

   @ManagedAttribute(
         description = "Returns the send buffer size.",
         displayName = "Send buffer size",
         dataType = DataType.TRAIT
   )
   @Override
   public int getSendBufferSize() {
      return configuration.sendBufSize();
   }

   @ManagedAttribute(
         description = "Returns the receive buffer size.",
         displayName = "Receive buffer size",
         dataType = DataType.TRAIT
   )
   @Override
   public int getReceiveBufferSize() {
      return configuration.recvBufSize();
   }

   @ManagedAttribute(
         description = "Returns a count of active connections this server.",
         displayName = "Local active connections"
   )
   @Override
   public int getNumberOfLocalConnections() {
      return connectionStats.getNumberOfLocalConnections();
   }

   @ManagedAttribute(
         description = "Returns a count of active connections in the cluster. " +
               "This operation will make remote calls to aggregate results, " +
               "so latency might have an impact on the speed of calculation of this attribute.",
         displayName = "Cluster-wide number of active connections",
         clusterWide = true
   )
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

   @Override
   public CompletionStage<Void> closeChannels(ChannelMatcher channelMatcher) {
      CompletableFuture<Void> closed = new CompletableFuture<>();
      acceptedChannels
            .close(channelMatcher)
            .addListener((ChannelGroupFutureListener) channelFutures -> closed.complete(null));
      return closed;
   }

   @Override
   public ChannelGroup getAcceptedChannels() {
      return acceptedChannels;
   }

   private Class<? extends ServerChannel> getServerSocketChannel() {
      Class<? extends ServerChannel> channel = NativeTransport.serverSocketChannelClass();
      log.createdSocketChannel(channel.getName(), configuration.toString());
      return channel;
   }

   public static MultithreadEventLoopGroup buildEventLoop(int nThreads, ThreadFactory threadFactory,
         String configuration) {
      MultithreadEventLoopGroup eventLoop = NativeTransport.createEventLoopGroup(nThreads, threadFactory);
      log.createdNettyEventLoop(eventLoop.getClass().getName(), configuration);
      return eventLoop;
   }
}
