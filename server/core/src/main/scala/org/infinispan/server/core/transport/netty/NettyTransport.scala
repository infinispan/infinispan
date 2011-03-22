package org.infinispan.server.core.transport.netty

import java.net.SocketAddress
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.channel.ChannelDownstreamHandler
import org.jboss.netty.bootstrap.ServerBootstrap
import java.util.concurrent.Executors
import org.infinispan.server.core.transport.Transport
import scala.collection.JavaConversions._
import org.infinispan.server.core.{ProtocolServer, Logging}
import org.jboss.netty.util.{ThreadNameDeterminer, ThreadRenamingRunnable}
import org.infinispan.util.logging.LogFactory
import org.jboss.netty.logging.{InternalLoggerFactory, Log4JLoggerFactory}

/**
 * A Netty based transport.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyTransport(server: ProtocolServer, encoder: ChannelDownstreamHandler,
                     address: SocketAddress, masterThreads: Int, workerThreads: Int,
                     idleTimeout: Int, threadNamePrefix: String, tcpNoDelay: Boolean,
                     sendBufSize: Int, recvBufSize: Int) extends Transport {
   import NettyTransport._

   private val serverChannels = new DefaultChannelGroup(threadNamePrefix + "-Channels")
   val acceptedChannels = new DefaultChannelGroup(threadNamePrefix + "-Accepted")
   private val pipeline = new NettyChannelPipelineFactory(server, encoder, this, idleTimeout)
   private val factory = {
      if (workerThreads == 0)
         new NioServerSocketChannelFactory(masterExecutor, workerExecutor)
      else
         new NioServerSocketChannelFactory(masterExecutor, workerExecutor, workerThreads)
   }
   
   private lazy val masterExecutor = {
      if (masterThreads == 0) {
         if (isDebugEnabled) debug("Configured unlimited threads for master thread pool")
         Executors.newCachedThreadPool
      } else {
         if (isDebugEnabled) debug("Configured %d threads for master thread pool", masterThreads)
         Executors.newFixedThreadPool(masterThreads)
      }
   }

   private lazy val workerExecutor = {
      if (workerThreads == 0) {
         if (isDebugEnabled) debug("Configured unlimited threads for worker thread pool")
         Executors.newCachedThreadPool
      }
      else {
         if (isDebugEnabled) debug("Configured %d threads for worker thread pool", workerThreads)
         Executors.newFixedThreadPool(masterThreads)
      }
   }

   override def start {
      ThreadRenamingRunnable.setThreadNameDeterminer(new ThreadNameDeterminer {
         override def determineThreadName(currentThreadName: String, proposedThreadName: String): String = {
            val index = proposedThreadName.findIndexOf(_ == '#')
            val typeInFix =
               if (proposedThreadName contains "server worker") "ServerWorker-"
               else if (proposedThreadName contains "server boss") "ServerMaster-"
               else if (proposedThreadName contains "client worker") "ClientWorker-"
               else "ClientMaster-"
            // Set thread name to be: <prefix><ServerWorker-|ServerMaster-|ClientWorker-|ClientMaster-><number>
            val name = threadNamePrefix + typeInFix + proposedThreadName.substring(index + 1, proposedThreadName.length)
            if (isTraceEnabled)
               trace("Thread name will be %s, with current thread name being %s and proposed name being '%s'",
                  name, currentThread, proposedThreadName)
            name
         }
      })
      // Make netty use log4j, otherwise it goes to JDK logging.
      if (LogFactory.IS_LOG4J_AVAILABLE)
         InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)

      val bootstrap = new ServerBootstrap(factory)
      bootstrap.setPipelineFactory(pipeline)
      bootstrap.setOption("child.tcpNoDelay", tcpNoDelay) // Sets server side tcpNoDelay
      if (sendBufSize > 0)
         bootstrap.setOption("child.sendBufferSize", sendBufSize) // Sets server side send buffer
      if (recvBufSize > 0)
         bootstrap.setOption("receiveBufferSize", recvBufSize) // Sets server side receive buffer

      val ch = bootstrap.bind(address)
      serverChannels.add(ch)
   }

   override def stop {
      // We *pause* the acceptor so no new connections are made
      var future = serverChannels.unbind().awaitUninterruptibly();
      if (!future.isCompleteSuccess()) {
         warn("Server channel group did not completely unbind");
         for (ch <- asIterator(future.getGroup().iterator)) {
            if (ch.isBound()) {
               warn("%s is still bound to %s", ch, ch.getRemoteAddress());
            }
         }
      }

      workerExecutor.shutdown();
      serverChannels.close().awaitUninterruptibly();
      future = acceptedChannels.close().awaitUninterruptibly();
      if (!future.isCompleteSuccess()) {
         warn("Channel group did not completely close");
         for (ch <- asIterator(future.getGroup().iterator)) {
            if (ch.isBound()) {
               warn(ch + " is still connected to " + ch.getRemoteAddress());
            }
         }
      }
      pipeline.stop
      if (isDebugEnabled) debug("Channel group completely closed, release external resources");
      factory.releaseExternalResources();
   }

}

object NettyTransport extends Logging
