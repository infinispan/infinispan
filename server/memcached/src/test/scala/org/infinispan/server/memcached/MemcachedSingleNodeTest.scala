package org.infinispan.server.memcached

import test.MemcachedTestingUtil
import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.manager.CacheManager
import org.infinispan.test.fwk.TestCacheManagerFactory
import net.spy.memcached.MemcachedClient
import org.testng.annotations.{Test, AfterClass}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
abstract class MemcachedSingleNodeTest extends SingleCacheManagerTest with MemcachedTestingUtil {
   private var memcachedClient: MemcachedClient = _
   private var memcachedServer: MemcachedServer = _
   private val operationTimeout: Int = 60

   override def createCacheManager: CacheManager = {
      cacheManager = createTestCacheManager
      memcachedServer = startMemcachedTextServer(cacheManager)
      memcachedClient = createMemcachedClient(60000, server.getPort)
      return cacheManager
   }

   protected def createTestCacheManager: CacheManager = TestCacheManagerFactory.createLocalCacheManager

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      super.destroyAfterClass
      log.debug("Test finished, close memcached server", null)
      shutdownClient
      memcachedServer.stop
   }

   protected def client: MemcachedClient = memcachedClient

   protected def timeout: Int = operationTimeout

   protected def server: MemcachedServer = memcachedServer

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   protected def shutdownClient = memcachedClient.shutdown
}