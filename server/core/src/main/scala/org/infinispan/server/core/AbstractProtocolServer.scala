package org.infinispan.server.core

import java.net.InetSocketAddress
import transport.netty.{EncoderAdapter, NettyTransport}
import transport.Transport
import org.infinispan.server.core.VersionGenerator._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.Main._
import java.util.Properties
import org.infinispan.util.logging.Log
import org.infinispan.util.{TypedProperties, Util}

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
      val typedProps = TypedProperties.toTypedProperties(properties)
      val toStart = typedProps.getBooleanProperty("enabled", true)

      if (toStart) {
         this.host = typedProps.getProperty(PROP_KEY_HOST, HOST_DEFAULT)
         this.port = typedProps.getIntProperty(PROP_KEY_PORT, defaultPort)
         this.masterThreads = typedProps.getIntProperty(PROP_KEY_MASTER_THREADS, MASTER_THREADS_DEFAULT)
         if (masterThreads < 0)
            throw new IllegalArgumentException("Master threads can't be lower than 0: " + masterThreads)

         this.workerThreads = typedProps.getIntProperty(PROP_KEY_WORKER_THREADS, WORKER_THREADS_DEFAULT)
         if (workerThreads < 0)
            throw new IllegalArgumentException("Worker threads can't be lower than 0: " + masterThreads)

         this.cacheManager = cacheManager
         val idleTimeout = typedProps.getIntProperty(PROP_KEY_IDLE_TIMEOUT, IDLE_TIMEOUT_DEFAULT)
         if (idleTimeout < -1)
            throw new IllegalArgumentException("Idle timeout can't be lower than -1: " + idleTimeout)

         val tcpNoDelay = typedProps.getBooleanProperty(PROP_KEY_TCP_NO_DELAY, TCP_NO_DELAY_DEFAULT)

         val sendBufSize = typedProps.getIntProperty(PROP_KEY_SEND_BUF_SIZE, SEND_BUF_SIZE_DEFAULT)
         if (sendBufSize < 0) {
            throw new IllegalArgumentException("Send buffer size can't be lower than 0: " + sendBufSize)
         }

         val recvBufSize = typedProps.getIntProperty(PROP_KEY_RECV_BUF_SIZE, RECV_BUF_SIZE_DEFAULT)
         if (recvBufSize < 0) {
            throw new IllegalArgumentException("Send buffer size can't be lower than 0: " + sendBufSize)
         }

         // Register rank calculator before starting any cache so that we can capture all view changes
         cacheManager.addListener(getRankCalculatorListener)
         // Start default cache
         startDefaultCache
         val address = new InetSocketAddress(host, port)
         startTransport(address, idleTimeout, tcpNoDelay, sendBufSize, recvBufSize)
      }
   }

   def startTransport(address: InetSocketAddress, idleTimeout: Int, tcpNoDelay: Boolean, sendBufSize: Int, recvBufSize: Int) {
      val encoder = getEncoder
      val nettyEncoder = if (encoder != null) new EncoderAdapter(encoder) else null
      transport = new NettyTransport(this, nettyEncoder, address, masterThreads, workerThreads, idleTimeout,
         threadNamePrefix, tcpNoDelay, sendBufSize, recvBufSize)
      transport.start
   }


   def start(propertiesFileName: String, cacheManager: EmbeddedCacheManager) {
      val propsObject = new TypedProperties()
      val stream = Util.loadResourceAsStream(propertiesFileName)
      propsObject.load(stream)
      start(propsObject, cacheManager)
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
