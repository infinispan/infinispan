package org.infinispan.server.memcached

import org.infinispan.test.MultipleCacheManagersTest
import test.MemcachedTestingUtil._
import net.spy.memcached.MemcachedClient
import org.testng.annotations.{AfterClass, Test}
import org.infinispan.server.core.test.ServerTestingUtil
import org.infinispan.manager.EmbeddedCacheManager
import scala.collection.mutable

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
abstract class MemcachedMultiNodeTest extends MultipleCacheManagersTest {

   // Type alias to shorten definitions
   type Cache = org.infinispan.Cache[String, Array[Byte]]

   val cacheName = "MemcachedReplSync"
   var servers: List[MemcachedServer] = List()
   var clients: List[MemcachedClient] = List()
   val cacheClient: mutable.Map[Cache, MemcachedClient] = mutable.Map[Cache, MemcachedClient]()

   val timeout: Int = 60

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers() {
      (0 until 2).foreach { i =>
         cacheManagers.add(createCacheManager(i))
      }

      waitForClusterToForm()
      servers = startMemcachedTextServer(cacheManagers.get(0), cacheName) :: servers
      servers = startMemcachedTextServer(cacheManagers.get(1), servers.head.getPort + 50, cacheName) :: servers
      servers.foreach { s =>
         val client = createMemcachedClient(60000, s.getPort)
         clients = client :: clients
         val cache = s.getCacheManager.getCache[String, Array[Byte]](cacheName)
         cacheClient += (cache -> client)
      }
   }

   protected def createCacheManager(index: Int): EmbeddedCacheManager

   @AfterClass(alwaysRun = true)
   override def destroy() {
      super.destroy()
      log.debug("Test finished, close Hot Rod server")
      clients.foreach(killMemcachedClient(_))
      servers.foreach(ServerTestingUtil.killServer(_))
   }

}
