package org.infinispan.server.hotrod

import org.infinispan.manager.CacheManager
import org.infinispan.server.core.AbstractProtocolServer
import org.infinispan.server.core.transport.{Decoder, Encoder}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class HotRodServer extends AbstractProtocolServer {

   protected override def getEncoder: Encoder = new HotRodEncoder

   protected override def getDecoder(cacheManager: CacheManager): Decoder = new HotRodDecoder(cacheManager)

}

//
////class HotRodServer(val host: String,
////                val port: Int,
////                val manager: CacheManager,
////                val masterThreads: Int,
////                val workerThreads: Int) {
////
////   import HotRodServer._
////
////   private var server: Server = _
////
////   def start {
////      val decoder = new GlobalDecoder
////      val nettyDecoder = new NettyNoStateDecoder(decoder)
////      val encoder = new Encoder410
////      val nettyEncoder = new NettyEncoder(encoder)
////      val commandHandler = new Handler(new CallerCache(manager))
////      server = new NettyServer(commandHandler, nettyDecoder, nettyEncoder, new InetSocketAddress(host, port),
////                               masterThreads, workerThreads, "HotRod")
////      server.start
////      info("Started Hot Rod bound to {0}:{1}", host, port)
////   }
////
////   def stop {
////      if (server != null) server.stop
////   }
////}