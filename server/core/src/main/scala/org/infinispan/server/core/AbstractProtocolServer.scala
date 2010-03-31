package org.infinispan.server.core

import java.net.InetSocketAddress
import transport.netty.{EncoderAdapter, DecoderAdapter, NettyTransport}
import transport.{Decoder, Encoder, Transport}
import org.infinispan.manager.{DefaultCacheManager, CacheManager}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class AbstractProtocolServer extends ProtocolServer {
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

      decoder = getDecoder(cacheManager)
      decoder.start
      encoder = getEncoder
      // TODO: add an IdleStateHandler so that idle connections are detected, this could help on malformed data
      // TODO: ... requests such as when the lenght of data is bigger than the expected data itself.
      val nettyDecoder = if (decoder != null) new DecoderAdapter(decoder) else null
      val nettyEncoder = if (encoder != null) new EncoderAdapter(encoder) else null
      val address =  new InetSocketAddress(host, port)
      // TODO change cache name 'default' to something more meaningful and dependent of protocol
      transport = new NettyTransport(nettyDecoder, nettyEncoder, address, masterThreads, workerThreads, "default")
      transport.start
   }

   override def stop {
      if (transport != null)
         transport.stop
      if (decoder != null)
         decoder.stop
//      cacheManager.stop
   }

   def getPort = port

   protected def getEncoder: Encoder

   protected def getDecoder(cacheManager: CacheManager): Decoder

}