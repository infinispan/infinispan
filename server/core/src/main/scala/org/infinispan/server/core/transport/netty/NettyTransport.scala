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

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyTransport(server: ProtocolServer, encoder: ChannelDownstreamHandler,
                     address: SocketAddress, masterThreads: Int, workerThreads: Int,
                     idleTimeout: Int, threadNamePrefix: String) extends Transport {
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
         debug("Configured unlimited threads for master thread pool")
         Executors.newCachedThreadPool
      } else {
         debug("Configured {0} threads for master thread pool", masterThreads)
         Executors.newFixedThreadPool(masterThreads)
      }
   }

   private lazy val workerExecutor = {
      if (workerThreads == 0) {
         debug("Configured unlimited threads for worker thread pool")
         Executors.newCachedThreadPool
      }
      else {
         debug("Configured {0} threads for worker thread pool", workerThreads)
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
            val name = threadNamePrefix + typeInFix + proposedThreadName.substring(index + 1, proposedThreadName.length)
            trace("Thread name will be {0}, with current thread name being {1} and proposed name being '{2}'",
               name, currentThread, proposedThreadName)
            name
         }
      })
      val bootstrap = new ServerBootstrap(factory);
      bootstrap.setPipelineFactory(pipeline);
      val ch = bootstrap.bind(address);
      serverChannels.add(ch);
   }

   override def stop {
      // We *pause* the acceptor so no new connections are made
      var future = serverChannels.unbind().awaitUninterruptibly();
      if (!future.isCompleteSuccess()) {
         warn("Server channel group did not completely unbind");
         for (ch <- asIterator(future.getGroup().iterator)) {
            if (ch.isBound()) {
               warn("{0} is still bound to {1}", ch, ch.getRemoteAddress());
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
      debug("Channel group completely closed, release external resources");
      factory.releaseExternalResources();
   }

}

object NettyTransport extends Logging