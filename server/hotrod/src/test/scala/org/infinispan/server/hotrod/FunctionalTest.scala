package org.infinispan.server.hotrod

import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.manager.CacheManager
import org.testng.annotations.{AfterClass, Test}
import java.lang.reflect.Method
import test.{Client, Utils}
import org.testng.Assert._
import org.infinispan.server.hotrod.Status._
import java.util.Arrays
import org.jboss.netty.channel.Channel

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
   private var ch: Channel = _

   override def createCacheManager: CacheManager = {
      val cacheManager = TestCacheManagerFactory.createLocalCacheManager
      server = createHotRodServer(cacheManager)
      server.start
      ch = connect("127.0.0.1", server.port)
      cacheManager
   }

   def testPutBasic(m: Method) {
      val status = put(ch, "__default", k(m) , 0, 0, v(m))
      assertSuccess(status)
   }

   def testGetBasic(m: Method) {
      val putSt = put(ch, "__default", k(m) , 0, 0, v(m))
      assertSuccess(putSt)
      val (getSt, actual) = get(ch, "__default", k(m))
      assertSuccess(getSt, v(m), actual)
   }

   def testGetDoesNotExist(m: Method) {
      val (getSt, actual) = get(ch, "__default", k(m))
      assertKeyDoesNotExist(getSt, actual)
   }

   private def assertSuccess(status: Status.Status) {
      assertTrue(status == Success, "Status should have been 'Success' but instead was: " + status)
   }

   private def assertSuccess(status: Status.Status, expected: Array[Byte], actual: Array[Byte]) {
      assertSuccess(status)
      assertTrue(Arrays.equals(expected, actual))
   }

   private def assertKeyDoesNotExist(status: Status.Status, actual: Array[Byte]) {
      assertTrue(status == KeyDoesNotExist, "Status should have been 'KeyDoesNotExist' but instead was: " + status)
      assertNull(actual)
   }

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      super.destroyAfterClass
      log.debug("Test finished, close memcached server", null)
      server.stop
   }

}