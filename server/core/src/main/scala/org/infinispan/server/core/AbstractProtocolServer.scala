package org.infinispan.server.core

import java.net.InetSocketAddress
import transport.netty.{EncoderAdapter, NettyTransport}
import transport.Transport
import org.infinispan.server.core.VersionGenerator._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.Main._
import java.util.Properties

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

   override def start(properties: Properties, cacheManager: EmbeddedCacheManager) {
      this.host = properties.getProperty(PROP_KEY_HOST)
      this.port = properties.getProperty(PROP_KEY_PORT).toInt
      this.masterThreads = properties.getProperty(PROP_KEY_MASTER_THREADS, MASTER_THREADS_DEFAULT).toInt
      this.workerThreads = properties.getProperty(PROP_KEY_WORKER_THREADS, WORKER_THREADS_DEFAULT).toInt
      this.cacheManager = cacheManager
      val idleTimeout = properties.getProperty(PROP_KEY_IDLE_TIMEOUT, IDLE_TIMEOUT_DEFAULT).toInt
      val tcpNoDelay = properties.getProperty(PROP_KEY_TCP_NO_DELAY, TCP_NO_DELAY_DEFAULT).toBoolean
      val sendBufSize = properties.getProperty(PROP_KEY_SEND_BUF_SIZE, SEND_BUF_SIZE_DEFAULT).toInt
      val recvBufSize = properties.getProperty(PROP_KEY_RECV_BUF_SIZE, RECV_BUF_SIZE_DEFAULT).toInt

      // Register rank calculator before starting any cache so that we can capture all view changes
      cacheManager.addListener(getRankCalculatorListener)
      // Start default cache
      startDefaultCache
      val address =  new InetSocketAddress(host, port)
      val encoder = getEncoder
      val nettyEncoder = if (encoder != null) new EncoderAdapter(encoder) else null
      transport = new NettyTransport(this, nettyEncoder, address, masterThreads, workerThreads, idleTimeout,
         threadNamePrefix, tcpNoDelay, sendBufSize, recvBufSize)
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
