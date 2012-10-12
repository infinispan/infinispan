/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.core.transport

import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.bootstrap.ServerBootstrap
import scala.collection.JavaConversions._
import org.infinispan.server.core.ProtocolServer
import org.infinispan.util.Util
import org.jboss.netty.util.{ThreadNameDeterminer, ThreadRenamingRunnable}
import org.jboss.netty.logging.{InternalLoggerFactory, Log4JLoggerFactory}
import org.infinispan.server.core.logging.Log
import java.util.concurrent.atomic.AtomicLong
import org.jboss.netty.channel.{WriteCompletionEvent, MessageEvent, ChannelDownstreamHandler}
import org.jboss.netty.buffer.ChannelBuffer
import java.net.InetSocketAddress
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.distexec.{DistributedCallable, DefaultExecutorService}
import org.infinispan.Cache
import java.util
import org.infinispan.jmx.JmxUtil
import javax.management.ObjectName
import util.concurrent.{TimeUnit, Executors}

/**
 * A Netty based transport.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyTransport(server: ProtocolServer, encoder: ChannelDownstreamHandler,
                     address: InetSocketAddress, workerThreads: Int,
                     idleTimeout: Int, threadNamePrefix: String, tcpNoDelay: Boolean,
                     sendBufSize: Int, recvBufSize: Int, cacheManager: EmbeddedCacheManager)
        extends Transport with Log {

   private val serverChannels = new DefaultChannelGroup(threadNamePrefix + "-Channels")
   val acceptedChannels = new DefaultChannelGroup(threadNamePrefix + "-Accepted")
   private val pipeline =
      if (idleTimeout > 0)
         new TimeoutEnabledChannelPipelineFactory(server, encoder, this, idleTimeout)
      else // Idle timeout logic is disabled with -1 or 0 values
         new NettyChannelPipelineFactory(server, encoder, this)

   private val masterExecutor = Executors.newCachedThreadPool
   private val workerExecutor = Executors.newCachedThreadPool
   private val factory = new NioServerSocketChannelFactory(masterExecutor, workerExecutor, workerThreads)

   private val totalBytesWritten, totalBytesRead = new AtomicLong
   private val isTrace = isTraceEnabled
   private val isGlobalStatsEnabled =
      cacheManager.getCacheManagerConfiguration.globalJmxStatistics().enabled()

   override def start() {
      ThreadRenamingRunnable.setThreadNameDeterminer(new ThreadNameDeterminer {
         override def determineThreadName(currentThreadName: String, proposedThreadName: String): String = {
            val index = proposedThreadName.indexWhere(_ == '#')
            val typeInFix =
               if (proposedThreadName contains "server worker") "ServerWorker-"
               else if (proposedThreadName contains "server boss") "ServerMaster-"
               else if (proposedThreadName contains "client worker") "ClientWorker-"
               else "ClientMaster-"
            // Set thread name to be: <prefix><ServerWorker-|ServerMaster-|ClientWorker-|ClientMaster-><number>
            val name = threadNamePrefix + typeInFix + proposedThreadName.substring(index + 1, proposedThreadName.length)
            if (isTrace)
               trace("Thread name will be %s, with current thread name being %s and proposed name being '%s'",
                  name, Thread.currentThread, proposedThreadName)
            name
         }
      })
      // Make netty use log4j, otherwise it goes to JDK logging.
      if (isLog4jAvailable)
         InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)

      val bootstrap = new ServerBootstrap(factory)
      bootstrap.setPipelineFactory(pipeline)
      bootstrap.setOption("child.tcpNoDelay", tcpNoDelay) // Sets server side tcpNoDelay
      if (sendBufSize > 0)
         bootstrap.setOption("child.sendBufferSize", sendBufSize) // Sets server side send buffer
      if (recvBufSize > 0)
         bootstrap.setOption("child.receiveBufferSize", recvBufSize) // Sets server side receive buffer

      val ch = bootstrap.bind(address)
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
      var future = serverChannels.unbind().awaitUninterruptibly()
      if (!future.isCompleteSuccess) {
         logServerDidNotUnbind
         for (ch <- asScalaIterator(future.getGroup.iterator)) {
            if (ch.isBound) {
               logChannelStillBound(ch, ch.getRemoteAddress)
            }
         }
      }

      workerExecutor.shutdown()
      serverChannels.close().awaitUninterruptibly()
      future = acceptedChannels.close().awaitUninterruptibly()
      if (!future.isCompleteSuccess) {
         logServerDidNotClose
         for (ch <- asScalaIterator(future.getGroup.iterator)) {
            if (ch.isBound) {
               logChannelStillConnected(ch, ch.getRemoteAddress)
            }
         }
      }
      pipeline.stop
      if (isDebugEnabled)
         debug("Channel group completely closed, release external resources")
      factory.releaseExternalResources()
   }

   override def getTotalBytesWritten: String = totalBytesWritten.toString

   override def getTotalBytesRead: String = totalBytesRead.toString

   override def getHostName = address.getHostName

   override def getPort = address.getPort.toString

   override def getNumberWorkerThreads = workerThreads.toString

   override def getIdleTimeout = idleTimeout.toString

   override def getTpcNoDelay = tcpNoDelay.toString

   override def getSendBufferSize = sendBufSize.toString

   override def getReceiveBufferSize = recvBufSize.toString

   override def getNumberOfLocalConnections: java.lang.Integer =
      Integer.valueOf(acceptedChannels.size())

   override def getNumberOfGlobalConnections: java.lang.Integer = {
      if (needDistributedCalculation())
         calculateGlobalConnections
      else
         getNumberOfLocalConnections
   }

   private[core] def updateTotalBytesWritten(e: WriteCompletionEvent) {
      if (isGlobalStatsEnabled)
         incrementTotalBytesWritten(totalBytesWritten, e)
   }

   private def incrementTotalBytesWritten(base: AtomicLong, e: WriteCompletionEvent) {
      if (isGlobalStatsEnabled)
         base.addAndGet(e.getWrittenAmount)
   }

   private[core] def updateTotalBytesRead(e: MessageEvent) {
      if (isGlobalStatsEnabled)
         incrementTotalBytesRead(totalBytesRead, e)
   }

   private def incrementTotalBytesRead(base: AtomicLong, e: MessageEvent) {
      if (isGlobalStatsEnabled)
         base.addAndGet(e.getMessage.asInstanceOf[ChannelBuffer].readableBytes)
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