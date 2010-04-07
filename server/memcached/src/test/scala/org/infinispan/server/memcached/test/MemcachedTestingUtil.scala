package org.infinispan.server.memcached.test

import java.lang.reflect.Method
import net.spy.memcached.{DefaultConnectionFactory, MemcachedClient}
import java.util.Arrays
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import org.infinispan.manager.CacheManager
import org.infinispan.server.core.transport.Decoder
import org.infinispan.server.memcached.{MemcachedDecoder, MemcachedValue, MemcachedServer}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
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

   def startMemcachedTextServer(cacheManager: CacheManager): MemcachedServer =
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue)

   def startMemcachedTextServer(cacheManager: CacheManager, port: Int): MemcachedServer = {
      val server = new MemcachedServer
      server.start(host, port, cacheManager, 0, 0)
      server
   }

   def startMemcachedTextServer(cacheManager: CacheManager, cacheName: String): MemcachedServer = {
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue, cacheName)
   }

   def startMemcachedTextServer(cacheManager: CacheManager, port: Int, cacheName: String): MemcachedServer = {
      val server = new MemcachedServer {
         override def getDecoder: Decoder = {
            new MemcachedDecoder(getCacheManager, scheduler) {
               override def createCache = getCacheManager.getCache[String, MemcachedValue](cacheName)
            }
         }
      }
      server.start(host, port, cacheManager, 0, 0)
      server
   }
   
}

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(11211)
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}