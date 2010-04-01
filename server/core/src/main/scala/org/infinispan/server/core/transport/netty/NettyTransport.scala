package org.infinispan.server.core.transport.netty

import java.net.SocketAddress
import org.jboss.netty.channel.group.{DefaultChannelGroup, ChannelGroup}
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.channel.{ChannelUpstreamHandler, ChannelDownstreamHandler, ChannelFactory, ChannelPipelineFactory}
import org.jboss.netty.bootstrap.ServerBootstrap
import java.util.concurrent.{TimeUnit, Executors, ThreadFactory, ExecutorService}
import org.infinispan.server.core.transport.Transport
import org.infinispan.server.core.Logging
import scala.collection.JavaConversions._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyTransport(decoder: ChannelUpstreamHandler, encoder: ChannelDownstreamHandler,
                  address: SocketAddress, masterThreads: Int, workerThreads: Int, cacheName: String) extends Transport {
   import NettyTransport._

   val serverChannels = new DefaultChannelGroup(cacheName + "-channels")
   val acceptedChannels = new DefaultChannelGroup(cacheName + "-accepted")
   val pipeline = new NettyChannelPipelineFactory(decoder, encoder)
   val factory = {
      if (workerThreads == 0)
         new NioServerSocketChannelFactory(masterExecutor, workerExecutor)
      else
         new NioServerSocketChannelFactory(masterExecutor, workerExecutor, workerThreads)
   }
   
   lazy val masterExecutor = {
      val tf = new NamedThreadFactory(cacheName + '-' + "Master")
      if (masterThreads == 0) {
         debug("Configured unlimited threads for master thread pool")
         Executors.newCachedThreadPool(tf)
      } else {
         debug("Configured {0} threads for master thread pool", masterThreads)
         Executors.newFixedThreadPool(masterThreads, tf)
      }
   }
   //todo investigate the actual reason why multiple threads do not work
   lazy val workerExecutor =  {
      val tf = new NamedThreadFactory(cacheName + '-' + "Worker")
      Executors.newSingleThreadExecutor(tf)
   }
//   lazy val workerExecutor = {
//      val tf = new NamedThreadFactory(cacheName + '-' + "Worker")
//      if (workerThreads == 0) {
//         debug("Configured unlimited threads for worker thread pool")
//         Executors.newCachedThreadPool(tf)
//      }
//      else {
//         debug("Configured {0} threads for worker thread pool", workerThreads)
//         Executors.newFixedThreadPool(workerThreads, tf)
//      }
//   }

   override def start {
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
      debug("Channel group completely closed, release external resources");
      factory.releaseExternalResources();
   }

}

object NettyTransport extends Logging

private class NamedThreadFactory(val name: String) extends ThreadFactory {
   val threadCounter = new AtomicInteger

   override def newThread(r: Runnable): Thread = {
      var t = new Thread(r, System.getProperty("program.name") + "-" + name + '-' + threadCounter.incrementAndGet)
      t.setDaemon(true)
      t
   }
}
