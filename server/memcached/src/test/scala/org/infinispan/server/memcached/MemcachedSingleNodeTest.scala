package org.infinispan.server.memcached

import test.MemcachedTestingUtil
import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.test.fwk.TestCacheManagerFactory
import net.spy.memcached.MemcachedClient
import org.testng.annotations.{Test, AfterClass}
import org.infinispan.manager.EmbeddedCacheManager

/**
 * Base class for single node tests.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class MemcachedSingleNodeTest extends SingleCacheManagerTest with MemcachedTestingUtil {
   private var memcachedClient: MemcachedClient = _
   private var memcachedServer: MemcachedServer = _
   private val operationTimeout: Int = 60

   override def createCacheManager: EmbeddedCacheManager = {
      cacheManager = createTestCacheManager
      memcachedServer = startMemcachedTextServer(cacheManager)
      memcachedClient = createMemcachedClient(60000, server.getPort)
      return cacheManager
   }

   protected def createTestCacheManager: EmbeddedCacheManager = TestCacheManagerFactory.createLocalCacheManager

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