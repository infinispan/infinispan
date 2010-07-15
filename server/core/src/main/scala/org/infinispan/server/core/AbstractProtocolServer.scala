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
   protected var host: String = _
   protected var port: Int = _
   protected var masterThreads: Int = _
   protected var workerThreads: Int = _
   protected var transport: Transport = _
   protected var cacheManager: EmbeddedCacheManager = _

   def start(properties: Properties, cacheManager: EmbeddedCacheManager, defaultPort: Int) {
      this.host = properties.getProperty(PROP_KEY_HOST, HOST_DEFAULT)
      this.port = properties.getProperty(PROP_KEY_PORT, defaultPort.toString).toInt
      this.masterThreads = properties.getProperty(PROP_KEY_MASTER_THREADS, MASTER_THREADS_DEFAULT).toInt
      if (masterThreads < 0)
         throw new IllegalArgumentException("Master threads can't be lower than 0: " + masterThreads)

      this.workerThreads = properties.getProperty(PROP_KEY_WORKER_THREADS, WORKER_THREADS_DEFAULT).toInt
      if (workerThreads < 0)
         throw new IllegalArgumentException("Worker threads can't be lower than 0: " + masterThreads)
      
      this.cacheManager = cacheManager
      val idleTimeout = properties.getProperty(PROP_KEY_IDLE_TIMEOUT, IDLE_TIMEOUT_DEFAULT).toInt
      if (idleTimeout < -1)
         throw new IllegalArgumentException("Idle timeout can't be lower than -1: " + idleTimeout)

      val tcpNoDelayString = properties.getProperty(PROP_KEY_TCP_NO_DELAY, TCP_NO_DELAY_DEFAULT)
      val tcpNoDelay =
         try {
            tcpNoDelayString.toBoolean
         } catch {
            case n: NumberFormatException => {
               throw new IllegalArgumentException("TCP no delay flag switch must be a boolean: " + tcpNoDelayString)
            }
         }

      val sendBufSize = properties.getProperty(PROP_KEY_SEND_BUF_SIZE, SEND_BUF_SIZE_DEFAULT).toInt
      if (sendBufSize < 0) {
         throw new IllegalArgumentException("Send buffer size can't be lower than 0: " + sendBufSize)
      }
      
      val recvBufSize = properties.getProperty(PROP_KEY_RECV_BUF_SIZE, RECV_BUF_SIZE_DEFAULT).toInt
      if (recvBufSize < 0) {
         throw new IllegalArgumentException("Send buffer size can't be lower than 0: " + sendBufSize)
      }

      // Register rank calculator before starting any cache so that we can capture all view changes
      cacheManager.addListener(getRankCalculatorListener)
      // Start default cache
      startDefaultCache
      val address =  new InetSocketAddress(host, port)
      startTransport(address, idleTimeout, tcpNoDelay, sendBufSize, recvBufSize)
   }

   def startTransport(address: InetSocketAddress, idleTimeout: Int, tcpNoDelay: Boolean, sendBufSize: Int, recvBufSize: Int) {
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
