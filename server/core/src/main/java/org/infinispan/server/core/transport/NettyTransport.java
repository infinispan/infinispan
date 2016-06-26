package org.infinispan.server.core.transport;

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
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.logging.Log;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Netty based transport.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 4.1
 */
public class NettyTransport implements Transport {

   static private final Log log = LogFactory.getLog(NettyTransport.class, Log.class);

   static private final String UseEpollProperty = "infinispan.server.channel.epoll";
   static private final boolean IsLinux = System.getProperty("os.name").toLowerCase().startsWith("linux");
   static private final boolean EpollDisabled = System.getProperty(UseEpollProperty, "true").equalsIgnoreCase("false");
   static private final boolean useEPoll;

   static private final boolean isLog4jAvailable;

   static {
      boolean exception;
      try {
         Util.loadClassStrict("org.apache.log4j.Logger", Thread.currentThread().getContextClassLoader());
         exception = false;
      } catch (ClassNotFoundException e) {
         exception = true;
      }
      isLog4jAvailable = !exception;

      if (Epoll.isAvailable()) {
         useEPoll = !EpollDisabled && IsLinux;
      } else {
         log.epollNotAvailable(Epoll.unavailabilityCause().toString());
         useEPoll = false;
      }
   }

   public NettyTransport(InetSocketAddress address, ProtocolServerConfiguration configuration, String threadNamePrefix,
           EmbeddedCacheManager cacheManager) {
      this.address = address;
      this.configuration = configuration;
      this.threadNamePrefix = threadNamePrefix;
      this.cacheManager = cacheManager;

      // Need to initialize these in constructor since they require configuration
      masterGroup = buildEventLoop(1, new DefaultThreadFactory(threadNamePrefix + "ServerMaster"));
      workerGroup = buildEventLoop(0, new DefaultThreadFactory(threadNamePrefix + "ServerWorker"));

      isGlobalStatsEnabled = cacheManager.getCacheManagerConfiguration().globalJmxStatistics().enabled();
      serverChannels = new DefaultChannelGroup(threadNamePrefix + "-Channels", ImmediateEventExecutor.INSTANCE);
      acceptedChannels = new DefaultChannelGroup(threadNamePrefix + "-Accepted", ImmediateEventExecutor.INSTANCE);
   }

   public void initializeHandler(ChannelInitializer<Channel> handler) {
      this.handler = handler;
   }

   private ChannelInitializer<Channel> handler;
   private final InetSocketAddress address;
   private final ProtocolServerConfiguration configuration;
   private final String threadNamePrefix;
   private final EmbeddedCacheManager cacheManager;

   private final ChannelGroup serverChannels;
   final ChannelGroup acceptedChannels;

   private final EventLoopGroup masterGroup;
   private final EventLoopGroup workerGroup;

   private final AtomicLong totalBytesWritten = new AtomicLong();
   private final AtomicLong totalBytesRead = new AtomicLong();
   private final boolean isGlobalStatsEnabled;

   @Override
   public void start() {
      // Make netty use log4j, otherwise it goes to JDK logging.
      if (isLog4jAvailable)
         InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

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

      Channel ch;
      try {
         ch = bootstrap.bind(address).sync().channel();
      } catch (InterruptedException e) {
         throw new CacheException(e);
      }
      serverChannels.add(ch);
   }

   @Override
   public void stop() {
      // We *pause* the acceptor so no new connections are made
      ChannelGroupFuture future = serverChannels.close().awaitUninterruptibly();
      if (!future.isSuccess()) {
         log.serverDidNotUnbind();

         future.forEach(fut -> {
            Channel ch = fut.channel();
            if (ch.isActive()) {
               log.channelStillBound(ch, ch.remoteAddress());
            }
         });
      }

      future = acceptedChannels.close().awaitUninterruptibly();
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
         log.debug("Channel group completely closed, release external resources");
      masterGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
   }

   @Override
   public String getTotalBytesWritten() {
      return totalBytesWritten.toString();
   }

   @Override
   public String getTotalBytesRead() {
      return totalBytesRead.toString();
   }

   @Override
   public String getHostName() {
      return address.getHostName();
   }

   @Override
   public String getPort() {
      return Integer.toString(address.getPort());
   }

   @Override
   public String getNumberWorkerThreads() {
      return Integer.toString(configuration.workerThreads());
   }

   @Override
   public String getIdleTimeout() {
      return Integer.toString(configuration.idleTimeout());
   }

   @Override
   public String getTcpNoDelay() {
      return Boolean.toString(configuration.tcpNoDelay());
   }

   @Override
   public String getSendBufferSize() {
      return Integer.toString(configuration.sendBufSize());
   }

   @Override
   public String getReceiveBufferSize() {
      return Integer.toString(configuration.recvBufSize());
   }

   @Override
   public Integer getNumberOfLocalConnections() {
      return Integer.valueOf(acceptedChannels.size());
   }

   @Override
   public Integer getNumberOfGlobalConnections() {
      if (needDistributedCalculation()) {
         return calculateGlobalConnections();
      } else {
         return getNumberOfLocalConnections();
      }
   }

   public void updateTotalBytesWritten(int bytes) {
      if (isGlobalStatsEnabled)
         incrementTotalBytes(totalBytesWritten, bytes);
   }

   public void updateTotalBytesRead(int bytes) {
      if (isGlobalStatsEnabled)
         incrementTotalBytes(totalBytesRead, bytes);
   }

   private void incrementTotalBytes(AtomicLong base, int bytes) {
      if (isGlobalStatsEnabled)
         base.addAndGet(bytes);
   }


   private boolean needDistributedCalculation() {
      org.infinispan.remoting.transport.Transport transport = cacheManager.getTransport();
      return transport != null && transport.getMembers().size() > 1;
   }

   private Class<? extends ServerChannel> getServerSocketChannel() {
      Class<? extends ServerChannel> channel = useEPoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
      log.createdSocketChannel(channel.getName(), configuration.toString());
      return channel;
   }

   private EventLoopGroup buildEventLoop(int nThreads, DefaultThreadFactory threadFactory) {
      EventLoopGroup eventLoop = useEPoll ? new EpollEventLoopGroup(nThreads, threadFactory) :
              new NioEventLoopGroup(nThreads, threadFactory);
      log.createdNettyEventLoop(eventLoop.getClass().getName(), configuration.toString());
      return eventLoop;
   }

   private int calculateGlobalConnections() {
      Cache<Object, Object> cache = cacheManager.getCache();
      DistributedExecutorService exec = new DefaultExecutorService(cache);
      try {
         // Submit calculation task
         List<CompletableFuture<Integer>> results = exec.submitEverywhere(
                 new ConnectionAdderTask(threadNamePrefix));
         // Take all results and add them up with a bit of functional programming magic :)
         return results.stream().mapToInt(f -> {
            try {
               return f.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
               throw new CacheException(e);
            }
         }).sum();
      } finally {
         exec.shutdown();
      }
   }

   static class ConnectionAdderTask implements Serializable, DistributedCallable<Object, Object, Integer> {
      private final String serverName;

      Cache<Object, Object> cache;

      ConnectionAdderTask(String serverName) {
         this.serverName = serverName;
      }

      @Override
      public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
         this.cache = cache;
      }

      @Override
      public Integer call() throws Exception {
         GlobalConfiguration globalCfg = cache.getCacheManager().getCacheManagerConfiguration();
         String jmxDomain = globalCfg.globalJmxStatistics().domain();
         MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);
         try {
            ObjectName transportMBeanName = new ObjectName(
                    jmxDomain + ":type=Server,component=Transport,name=" + serverName);

            return (Integer) mbeanServer.getAttribute(transportMBeanName, "NumberOfLocalConnections");
         } catch (MBeanException | AttributeNotFoundException | InstanceNotFoundException | ReflectionException |
                 MalformedObjectNameException e) {
            throw new RuntimeException(e);
         }
      }
   }
}
