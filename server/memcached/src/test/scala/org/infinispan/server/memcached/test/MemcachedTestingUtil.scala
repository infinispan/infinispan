package org.infinispan.server.memcached.test

import net.spy.memcached.{DefaultConnectionFactory, MemcachedClient}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import org.infinispan.server.memcached.{MemcachedDecoder, MemcachedServer}
import org.infinispan.manager.EmbeddedCacheManager
import java.util

import org.infinispan.commons.logging.LogFactory
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder
import org.infinispan.server.memcached.logging.JavaLog

/**
 * Utils for Memcached tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object MemcachedTestingUtil {
   val log = LogFactory.getLog(getClass, classOf[JavaLog])

   def host = "127.0.0.1"

   def createMemcachedClient(timeout: Long, port: Int): MemcachedClient = {
      val d: DefaultConnectionFactory = new DefaultConnectionFactory {
         override def getOperationTimeout: Long = timeout
      }
      new MemcachedClient(d, util.Arrays.asList(new InetSocketAddress(host, port)))
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager): MemcachedServer =
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue)

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, port: Int): MemcachedServer = {
      val server = new MemcachedServer
      server.start(new MemcachedServerConfigurationBuilder().host(host).port(port).build(), cacheManager)
      server
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, cacheName: String): MemcachedServer = {
      startMemcachedTextServer(cacheManager, UniquePortThreadLocal.get.intValue, cacheName)
   }

   def startMemcachedTextServer(cacheManager: EmbeddedCacheManager, port: Int, cacheName: String): MemcachedServer = {
      val server = new MemcachedServer {

         override def getDecoder: MemcachedDecoder =
            new MemcachedDecoder(getCacheManager.getCache[String, Array[Byte]](cacheName).getAdvancedCache, scheduler, transport)

         override def startDefaultCache = getCacheManager.getCache(cacheName)
      }
      server.start(new MemcachedServerConfigurationBuilder().host(host).port(port).build(), cacheManager)
      server
   }

   def killMemcachedClient(client: MemcachedClient) {
      try {
         if (client != null) client.shutdown()
      }
      catch {
         case t: Throwable => {
            log.error("Error stopping client", t)
         }
      }
   }

   def killMemcachedServer(server: MemcachedServer) {
      if (server != null) server.stop
   }

}

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(16211)
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}
