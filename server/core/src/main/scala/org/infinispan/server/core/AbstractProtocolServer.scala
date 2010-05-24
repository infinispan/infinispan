package org.infinispan.server.core

import java.net.InetSocketAddress
import transport.netty.{EncoderAdapter, NettyTransport}
import transport.Transport
import org.infinispan.server.core.VersionGenerator._
import org.infinispan.manager.{EmbeddedCacheManager, CacheManager}

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
   private var cacheManager: EmbeddedCacheManager = _

   override def start(host: String, port: Int, cacheManager: EmbeddedCacheManager, masterThreads: Int, workerThreads: Int, idleTimeout: Int) {
      this.host = host
      this.port = port
      this.masterThreads = masterThreads
      this.workerThreads = workerThreads
      this.cacheManager = cacheManager

      // Register rank calculator before starting any cache so that we can capture all view changes
      cacheManager.addListener(getRankCalculatorListener)
      // Start default cache
      startDefaultCache
      val address =  new InetSocketAddress(host, port)
      val encoder = getEncoder
      val nettyEncoder = if (encoder != null) new EncoderAdapter(encoder) else null
      transport = new NettyTransport(this, nettyEncoder, address, masterThreads, workerThreads, idleTimeout, threadNamePrefix)
      transport.start
   }

   override def stop {
      if (transport != null)
         transport.stop
   }

   def getCacheManager = cacheManager

   def getHost = host

   def getPort = port

   def startDefaultCache = cacheManager.getCache
}
