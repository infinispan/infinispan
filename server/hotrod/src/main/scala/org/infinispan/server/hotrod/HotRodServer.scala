package org.infinispan.server.hotrod

import org.infinispan.manager.CacheManager
import org.infinispan.server.core.transport.netty.{NettyServer, NettyReplayingDecoder}
import java.net.InetSocketAddress
import org.infinispan.server.core.Server

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 3.5
 */

class HotRodServer(val host: String,
                val port: Int,
                val manager: CacheManager,
                val masterThreads: Int,
                val workerThreads: Int) {

   import HotRodServer._

   private var server: Server = null

   def start {
      var decoder = new GlobalDecoder
      val nettyDecoder = new NettyReplayingDecoder[NoState](decoder)
      val commandHandler = new Handler(new CallerCache(manager))
      server = new NettyServer(commandHandler, nettyDecoder, new InetSocketAddress(host, port),
                               masterThreads, workerThreads, "HotRod")
      server.start
      info("Started Hot Rod bound to {0}:{1}", host, port)
   }

   def stop {
      if (server != null) server.stop
   }
}

object HotRodServer extends Logging