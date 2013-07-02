package org.infinispan.server.memcached

import org.infinispan.test.MultipleCacheManagersTest
import test.MemcachedTestingUtil._
import net.spy.memcached.MemcachedClient
import org.testng.annotations.{AfterClass, Test}
import org.infinispan.server.core.test.ServerTestingUtil
import org.infinispan.manager.EmbeddedCacheManager

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
abstract class MemcachedMultiNodeTest extends MultipleCacheManagersTest {

   private val cacheName = "MemcachedReplSync"
   var servers: List[MemcachedServer] = List()
   var clients: List[MemcachedClient] = List()
   val timeout: Int = 60

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers() {
      (0 until 2).foreach { i =>
         cacheManagers.add(createCacheManager(i))
      }

      waitForClusterToForm()
      servers = startMemcachedTextServer(cacheManagers.get(0), cacheName) :: servers
      servers = startMemcachedTextServer(cacheManagers.get(1), servers.head.getPort + 50, cacheName) :: servers
      servers.foreach(s => clients = createMemcachedClient(60000, s.getPort) :: clients)
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
