package org.infinispan.server.core

import java.net.InetSocketAddress
import transport.netty.{EncoderAdapter, NettyTransport}
import transport.{Decoder, Encoder, Transport}
import org.infinispan.manager.CacheManager

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractProtocolServer(threadNamePrefix: String) extends ProtocolServer {
   private var host: String = _
   private var port: Int = _
   private var masterThreads: Int = _
   private var workerThreads: Int = _
   private var transport: Transport = _
   private var cacheManager: CacheManager = _
   private var decoder: Decoder = _
   private var encoder: Encoder = _

   override def start(host: String, port: Int, cacheManager: CacheManager, masterThreads: Int, workerThreads: Int) {
      this.host = host
      this.port = port
      this.masterThreads = masterThreads
      this.workerThreads = workerThreads
      this.cacheManager = cacheManager

      encoder = getEncoder
      // TODO: add an IdleStateHandler so that idle connections are detected, this could help on malformed data
      // TODO: ... requests such as when the lenght of data is bigger than the expected data itself.
      val nettyEncoder = if (encoder != null) new EncoderAdapter(encoder) else null
      val address =  new InetSocketAddress(host, port)
      transport = new NettyTransport(this, nettyEncoder, address, masterThreads, workerThreads, threadNamePrefix)
      transport.start
   }

   override def stop {
      if (transport != null)
         transport.stop
   }

   def getCacheManager = cacheManager

   def getPort = port

}