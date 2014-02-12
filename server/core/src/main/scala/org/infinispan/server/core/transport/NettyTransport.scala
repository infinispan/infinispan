package org.infinispan.server.core.transport

import io.netty.channel.group.DefaultChannelGroup
import scala.collection.JavaConversions._
import org.infinispan.server.core.ProtocolServer
import org.infinispan.commons.util.Util
import org.infinispan.server.core.logging.Log
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.net.InetSocketAddress
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.distexec.{DistributedCallable, DefaultExecutorService}
import org.infinispan.Cache
import java.util
import org.infinispan.jmx.JmxUtil
import javax.management.ObjectName
import java.util.concurrent.{ThreadFactory, TimeUnit}
import org.infinispan.server.core.configuration.ProtocolServerConfiguration
import io.netty.util.concurrent.ImmediateEventExecutor
import io.netty.util.internal.logging.{Log4JLoggerFactory, InternalLoggerFactory}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.buffer.PooledByteBufAllocator

/**
 * A Netty based transport.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyTransport(server: ProtocolServer, handler: ChannelInitializer[Channel],
                     address: InetSocketAddress, configuration: ProtocolServerConfiguration, threadNamePrefix: String, cacheManager: EmbeddedCacheManager)
        extends Transport with Log {

   private val serverChannels = new DefaultChannelGroup(threadNamePrefix + "-Channels", ImmediateEventExecutor.INSTANCE)
   val acceptedChannels = new DefaultChannelGroup(threadNamePrefix + "-Accepted", ImmediateEventExecutor.INSTANCE)

   private val masterGroup = new NioEventLoopGroup(1, new InfinispanThreadFactory(threadNamePrefix + "ServerMaster"))
   private val workerGroup = new NioEventLoopGroup(configuration.workerThreads, new InfinispanThreadFactory(threadNamePrefix + "ServerWorker"))

   private val totalBytesWritten, totalBytesRead = new AtomicLong
   private val isGlobalStatsEnabled =
      cacheManager.getCacheManagerConfiguration.globalJmxStatistics().enabled()

   override def start() {
      // Make netty use log4j, otherwise it goes to JDK logging.
      if (isLog4jAvailable)
         InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)

      val bootstrap = new ServerBootstrap()
      bootstrap.group(masterGroup, workerGroup)
      bootstrap.channel(classOf[NioServerSocketChannel])
      bootstrap.childHandler(handler)
      bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
      bootstrap.childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, configuration.tcpNoDelay) // Sets server side tcpNoDelay
      if (configuration.sendBufSize > 0)
         bootstrap.childOption[java.lang.Integer](ChannelOption.SO_SNDBUF, configuration.sendBufSize) // Sets server side send buffer
      if (configuration.recvBufSize > 0)
         bootstrap.childOption[java.lang.Integer](ChannelOption.SO_RCVBUF, configuration.recvBufSize) // Sets server side receive buffer

      val ch = bootstrap.bind(address).sync().channel()
      serverChannels.add(ch)
   }

   private def isLog4jAvailable: Boolean = {
      try {
         Util.loadClassStrict("org.apache.log4j.Logger",
            Thread.currentThread().getContextClassLoader)
         true
      } catch {
        case cnfe: ClassNotFoundException => false
      }
   }

   override def stop() {
      // We *pause* the acceptor so no new connections are made
      var future = serverChannels.close().awaitUninterruptibly()
      if (!future.isSuccess) {
         logServerDidNotUnbind
         for (ch <- asScalaIterator(future.group().iterator)) {
            if (ch.isActive) {
               logChannelStillBound(ch, ch.remoteAddress())
            }
         }
      }

      serverChannels.close().awaitUninterruptibly()
      future = acceptedChannels.close().awaitUninterruptibly()
      if (!future.isSuccess) {
         logServerDidNotClose
         for (ch <- asScalaIterator(future.group().iterator)) {
            if (ch.isActive) {
               logChannelStillConnected(ch, ch.remoteAddress)
            }
         }
      }
      if (isDebugEnabled)
         debug("Channel group completely closed, release external resources")
      masterGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
   }

   override def getTotalBytesWritten: String = totalBytesWritten.toString

   override def getTotalBytesRead: String = totalBytesRead.toString

   override def getHostName = address.getHostName

   override def getPort = address.getPort.toString

   override def getNumberWorkerThreads = configuration.workerThreads.toString

   override def getIdleTimeout = configuration.idleTimeout.toString

   override def getTcpNoDelay = configuration.tcpNoDelay.toString

   override def getSendBufferSize = configuration.sendBufSize.toString

   override def getReceiveBufferSize = configuration.recvBufSize.toString

   override def getNumberOfLocalConnections: java.lang.Integer =
      Integer.valueOf(acceptedChannels.size())

   override def getNumberOfGlobalConnections: java.lang.Integer = {
      if (needDistributedCalculation())
         calculateGlobalConnections
      else
         getNumberOfLocalConnections
   }

   private[core] def updateTotalBytesWritten(bytes: Int) {
      if (isGlobalStatsEnabled)
         incrementTotalBytesWritten(totalBytesWritten, bytes)
   }

   private def incrementTotalBytesWritten(base: AtomicLong, bytes: Int) {
      if (isGlobalStatsEnabled)
         base.addAndGet(bytes)
   }

   private[core] def updateTotalBytesRead(bytes: Int) {
      if (isGlobalStatsEnabled)
         incrementTotalBytesRead(totalBytesRead, bytes)
   }

   private def incrementTotalBytesRead(base: AtomicLong, bytes: Int) {
      if (isGlobalStatsEnabled)
         base.addAndGet(bytes)
   }

   private def needDistributedCalculation(): Boolean = {
      val transport = cacheManager.getTransport
      transport != null && transport.getMembers.size() > 1
   }

   private def calculateGlobalConnections: java.lang.Integer = {
      // TODO: Assumes default clustered cache but could be that no caches are clustered
      // TODO: A way to make distributed calls with only a cache manager would be ideal
      val cache = cacheManager.getCache[AnyRef, AnyRef]()
      val exec = new DefaultExecutorService(cache)
      try {
         // Submit calculation task
         val results = exec.submitEverywhere(
            new ConnectionAdderTask(threadNamePrefix)).iterator()
         // Take all results and add them up with a bit of functional programming magic :)
         asScalaIterator(results).map(_.get(30, TimeUnit.SECONDS)).reduceLeft(_ + _)
      } finally {
         exec.shutdown()
      }
   }


   private [this] class InfinispanThreadFactory(prefix: String) extends ThreadFactory {
      private final val nextId: AtomicInteger = new AtomicInteger
      def newThread(r: Runnable): Thread = {
         val thread = new Thread(r, prefix + "-" + nextId.incrementAndGet())
         thread
      }
   }
}
// TODO: Could be generalised to calculate any jmx params cluster wide
class ConnectionAdderTask(serverName: String)
        extends DistributedCallable[AnyRef, AnyRef, java.lang.Integer] with Serializable {

   @transient var cache: Cache[AnyRef, AnyRef] = _

   def call(): java.lang.Integer = {
      val globalCfg = cache.getCacheManager.getCacheManagerConfiguration
      val jmxDomain = globalCfg.globalJmxStatistics().domain()
      val mbeanServer = JmxUtil.lookupMBeanServer(globalCfg)
      val transportMBeanName = new ObjectName(
         jmxDomain + ":type=Server,component=Transport,name=" + serverName)

      mbeanServer.getAttribute(transportMBeanName, "NumberOfLocalConnections")
              .asInstanceOf[java.lang.Integer]
   }

   def setEnvironment(cache: Cache[AnyRef, AnyRef], inputKeys: util.Set[AnyRef]) {
      this.cache = cache
   }

}

