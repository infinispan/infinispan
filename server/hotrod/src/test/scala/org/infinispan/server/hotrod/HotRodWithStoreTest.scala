package org.infinispan.server.hotrod

import java.lang.reflect.Method

import org.infinispan.configuration.cache.ConfigurationBuilder
import org.infinispan.context.Flag
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.testng.Assert._
import org.testng.annotations.Test

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodWithStoreTest")
class HotRodWithStoreTest extends HotRodSingleNodeTest {

   override def createCacheManager: EmbeddedCacheManager = {
      val cacheManager = createTestCacheManager
      val builder = new ConfigurationBuilder
      builder.persistence()
            .addStore(classOf[DummyInMemoryStoreConfigurationBuilder])
            .storeName(getClass.getName)
      cacheManager.defineConfiguration(cacheName, builder.build())
      advancedCache = cacheManager.getCache[Array[Byte], Array[Byte]](cacheName).getAdvancedCache
      cacheManager
   }

   def testSize(m: Method): Unit = {
      val sizeStart = client.size()
      assertStatus(sizeStart, Success)
      assertEquals(0, sizeStart.size)
      for (i <- 0 until 20) client.assertPut(m, s"k-$i", s"v-$i")

      // Clear contents from memory
      advancedCache.withFlags(Flag.SKIP_CACHE_STORE).clear()

      val sizeEnd = client.size()
      assertStatus(sizeEnd, Success)
      assertEquals(20, sizeEnd.size)
   }

}
