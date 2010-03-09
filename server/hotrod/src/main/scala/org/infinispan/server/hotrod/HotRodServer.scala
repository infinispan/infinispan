package org.infinispan.server.hotrod

import org.infinispan.manager.CacheManager
import java.net.InetSocketAddress
import org.infinispan.server.core.transport.netty.{NettyNoStateDecoder, NettyEncoder, NettyServer, NettyDecoder}
import org.infinispan.server.core.transport.Server

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

   private var server: Server = _

   def start {
      val decoder = new GlobalDecoder
      val nettyDecoder = new NettyNoStateDecoder(decoder)      
      val encoder = new Encoder410
      val nettyEncoder = new NettyEncoder(encoder)
      val commandHandler = new Handler(new CallerCache(manager))
      server = new NettyServer(commandHandler, nettyDecoder, nettyEncoder, new InetSocketAddress(host, port),
                               masterThreads, workerThreads, "HotRod")
      server.start
      info("Started Hot Rod bound to {0}:{1}", host, port)
   }

   def stop {
      if (server != null) server.stop
   }
}

object HotRodServer extends Logging