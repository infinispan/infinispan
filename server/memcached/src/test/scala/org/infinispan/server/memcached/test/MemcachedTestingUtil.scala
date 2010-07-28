package org.infinispan.server.memcached.test

import java.lang.reflect.Method
import net.spy.memcached.{DefaultConnectionFactory, MemcachedClient}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.server.core.transport.Decoder
import org.infinispan.server.memcached.{MemcachedDecoder, MemcachedValue, MemcachedServer}
import org.infinispan.manager.EmbeddedCacheManager
import java.util.{Properties, Arrays}
import org.infinispan.server.core.Main._

/**
 * Utils for Memcached tests.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
trait MemcachedTestingUtil {
   def host = "127.0.0.1"

   def k(m: Method, prefix: String): String = prefix + m.getName

   def v(m: Method, prefix: String): String = prefix + m.getName

   def k(m: Method): String = k(m, "k-")

   def v(m: Method): String = v(m, "v-")

   def createMemcachedClient(timeout: Long, port: Int): MemcachedClient = {
      var d: DefaultConnectionFactory = new DefaultConnectionFactory {
         override def getOperationTimeout: Long = timeout
      }
      return new MemcachedClient(d, Arrays.asList(new InetSocketAddress(host, port)))
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager): MemcachedServer =
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue)

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, port: Int): MemcachedServer = {
      val server = new MemcachedServer
      server.start(getProperties(host, port), cacheManager)
      server
   }

   private def getProperties(host: String, port: Int): Properties = {
      val properties = new Properties
      properties.setProperty(PROP_KEY_HOST, host)
      properties.setProperty(PROP_KEY_PORT, port.toString)
      properties
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, cacheName: String): MemcachedServer = {
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue, cacheName)
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, port: Int, cacheName: String): MemcachedServer = {
      val server = new MemcachedServer {
         override def getDecoder: Decoder =
            new MemcachedDecoder(getCacheManager.getCache[String, MemcachedValue](cacheName), scheduler)

         override def startDefaultCache = getCacheManager.getCache(cacheName)
      }
      server.start(getProperties(host, port), cacheManager)
      server
   }
   
}

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(12211)
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}
