package org.infinispan.server.hotrod

import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.manager.CacheManager
import org.testng.annotations.{AfterClass, Test}
import java.lang.reflect.Method
import test.{Client, Utils}
import org.testng.Assert._

/**
 * TODO: Document
 *
 * Note: It appears that optional parameters in annotations result in compiler errors.
 * This has been solved in Scala 2.8.0.Beta1, so use that compiler,
 * otherwise this class won't compile.
 * https://lampsvn.epfl.ch/trac/scala/ticket/1810
 *
 * Keep an eye on that for @Test and @AfterClass annotations
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.FunctionalTest")
class FunctionalTest extends SingleCacheManagerTest with Utils with Client {
   private var server: HotRodServer = _

   override def createCacheManager: CacheManager = {
      val cacheManager = TestCacheManagerFactory.createLocalCacheManager
      server = createHotRodServer(cacheManager)
      server.start
      cacheManager
   }

   def testPutBasic(m: Method) {
      assertTrue(connect("127.0.0.1", server.port))
      val status = put("__default", k(m) , 0, 0, v(m))
      assertTrue(status == 0, "Status should have been 0 but instead was: " + status)
   }

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      log.debug("Test finished, close memcached server", null)
      server.stop
   }

}